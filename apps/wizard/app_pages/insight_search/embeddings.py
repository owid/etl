import os
import time
from typing import Any, Dict, Optional

import streamlit as st
import torch
from joblib import Memory
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger
from tqdm.auto import tqdm

from etl.paths import CACHE_DIR

memory = Memory(CACHE_DIR, verbose=0)

# Initialize log.
log = get_logger()


def get_default_device() -> str:
    return "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"


# Set the default device. We use CPU on our servers, but you can change this to "cuda" if you have a GPU.
DEVICE = os.environ.get("DEVICE", get_default_device())

torch.set_default_device(DEVICE)


@memory.cache
def get_model(model_name: str = "all-MiniLM-L6-v2") -> SentenceTransformer:
    "Load the pre-trained model."
    with st.spinner("Loading model..."):
        model = SentenceTransformer(model_name)
    return model


def get_embeddings(
    model: SentenceTransformer,
    texts: list[str],
    model_name: Optional[str] = None,
    batch_size=32,
    workers=1,
) -> list:
    log.info("get_embeddings.start", n_embeddings=len(texts))
    t = time.time()

    # Get model name
    if model_name is None:
        # NOTE: this is a bit of a hack
        model_name = model.tokenizer.name_or_path.split("/")[-1]

    cache_file_keys = CACHE_DIR / f"embeddings_{model_name}.keys"
    cache_file_keys.touch(exist_ok=True)

    cache_file_tensor = CACHE_DIR / f"embeddings_{model_name}.pt"
    if not cache_file_tensor.exists():
        embedding_dim = model.get_sentence_embedding_dimension()
        torch.save(torch.empty((0, embedding_dim)), cache_file_tensor)  # type: ignore

    # Load embeddings and keys
    embeddings = torch.load(cache_file_tensor).to(DEVICE)
    with open(str(cache_file_keys).replace(".pt", ".keys"), "r") as f:
        keys = [line.strip() for line in f]

    missing_texts = []

    # Check cache for existing embeddings
    keys_set = set(keys)
    for idx, text in tqdm(enumerate(texts), total=len(texts), desc="Checking cache"):
        if text not in keys_set:
            missing_texts.append(text)

    log.info(
        "get_embeddings.encoding",
        n_embeddings=len(texts),
        in_cache=len(texts) - len(missing_texts),
    )

    # Encode missing texts
    if missing_texts:
        if workers > 1:
            # Start the multiprocessing pool
            pool = model.start_multi_process_pool(target_devices=workers * [DEVICE])
            # Encode sentences using multiprocessing
            batch_embeddings = model.encode_multi_process(
                missing_texts,
                pool,
                batch_size=batch_size,
                # precision="float32"
            )
            # Close the multiprocessing pool
            model.stop_multi_process_pool(pool)
        else:
            # Use single process encoding
            batch_embeddings = model.encode(
                missing_texts,
                convert_to_tensor=True,
                batch_size=batch_size,
                show_progress_bar=True,
                device=DEVICE,
            )

        # Convert batch_embeddings to torch tensor if necessary
        if not isinstance(batch_embeddings, torch.Tensor):
            batch_embeddings = torch.from_numpy(batch_embeddings)

        # TODO: should embeddings be on CPU or GPU?
        # Ensure batch_embeddings are on CPU
        # batch_embeddings = batch_embeddings.cpu()

        # Update keys and embeddings
        keys.extend(missing_texts)
        embeddings = torch.cat([embeddings, batch_embeddings])

        # Save updated cache to pickle file
        torch.save(embeddings, cache_file_tensor)
        with open(cache_file_keys, "w") as f:
            f.writelines([key + "\n" for key in keys])

    # Create a mapping from keys to indices
    key_to_index = {key: idx for idx, key in enumerate(keys)}

    # Get requested embeddings in order
    indices = [key_to_index[text] for text in texts]
    req_embeddings = embeddings[indices]

    log.info("get_embeddings.end", t=time.time() - t)

    return req_embeddings.unbind(dim=0)


# TODO: caching isn't working properly when on different devices
# @st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_insights_embeddings(_model, insights: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the title, body and authors of each insight into a single string.
        insights_texts = [
            insight["title"] + " " + insight["raw_text"] + " " + " ".join(insight["authors"]) for insight in insights
        ]

        return get_embeddings(_model, insights_texts)


def get_sorted_documents_by_similarity(
    model: SentenceTransformer, input_string: str, insights: list[Dict[str, str]], embeddings: list
) -> list[Dict[str, Any]]:
    """Ingests an input string and a list of documents, returning the list of documents sorted by their semantic similarity to the input string."""
    _insights = insights.copy()

    # Encode the input string and the document texts.
    input_embedding = model.encode(input_string, convert_to_tensor=True, device=DEVICE)

    # Compute the cosine similarity between the input and each document.
    def _get_score(a, b):
        score = util.pytorch_cos_sim(a, b).item()
        score = (score + 1) / 2
        return score

    similarities = [_get_score(input_embedding, doc_embedding) for doc_embedding in embeddings]  # type: ignore

    # Attach the similarity scores to the documents.
    for i, doc in enumerate(_insights):
        doc["similarity"] = similarities[i]  # type: ignore

    # Sort the documents by descending similarity score.
    sorted_documents = sorted(_insights, key=lambda x: x["similarity"], reverse=True)

    return sorted_documents
