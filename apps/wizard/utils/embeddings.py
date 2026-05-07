import os
import pickle
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, TypeVar

import torch
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger

from etl.config import DOCS_BUILD
from etl.paths import CACHE_DIR

# Initialize log.
log = get_logger()

DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"


def set_device() -> str:
    default_device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

    # Set the default device. We use CPU on our servers, but you can change this to "cuda" if you have a GPU.
    device = os.environ.get("DEVICE", default_device)
    if DOCS_BUILD:
        try:
            torch.set_default_device(device)
        except RuntimeError:
            # If was already called, this can happen in streamlit apps
            pass

    return device


# Set cpu as default, other devices are causing problems
if "DEVICE" not in os.environ:
    os.environ["DEVICE"] = "cpu"

DEVICE = set_device()


def get_model(model_name: str = DEFAULT_MODEL_NAME) -> SentenceTransformer:
    """Load the pre-trained model.

    Uses the HuggingFace on-disk cache (not joblib): first call downloads the model,
    subsequent calls skip the Hub roundtrip via local_files_only=True (~0.1s).
    """
    try:
        return SentenceTransformer(model_name, local_files_only=True)
    except OSError:
        return SentenceTransformer(model_name)


@dataclass
class Doc:
    similarity: float | None = field(default=None, init=False)

    def text(self) -> str:
        raise NotImplementedError


TDoc = TypeVar("TDoc", bound="Doc")


class EmbeddingsModel(Generic[TDoc]):
    # documents and their embeddings
    docs: list[TDoc]
    embeddings: torch.Tensor

    def __init__(self, model: SentenceTransformer, model_name: str | None = None) -> None:
        # Derive name from the model so it cannot drift from the embeddings file.
        # Callers may override to namespace cache files (e.g. "sim_charts_title").
        self.model = model
        self.model_name = model_name or model.tokenizer.name_or_path.split("/")[-1]

    @property
    def cache_file_keys(self) -> Path:
        return CACHE_DIR / f"embeddings_{self.model_name}.keys.pkl"

    @property
    def cache_file_tensor(self) -> Path:
        return CACHE_DIR / f"embeddings_{self.model_name}.pt"

    def _load(self) -> tuple[list[str], torch.Tensor]:
        """Load embeddings and keys from cache files."""
        if not self.cache_file_keys.exists():
            keys = []
            embeddings = None
        else:
            # Load embeddings and keys
            embeddings = torch.load(self.cache_file_tensor).to(DEVICE)
            # Load keys from pickle file
            with open(self.cache_file_keys, "rb") as f:
                keys = pickle.load(f)

        return keys, embeddings  # ty: ignore

    def _save(self, keys: list[str], embeddings: torch.Tensor) -> None:
        """Save embeddings and keys to cache files."""
        torch.save(embeddings, self.cache_file_tensor)
        with open(self.cache_file_keys, "wb") as f:
            pickle.dump(keys, f)

    def fit(self, docs: list[TDoc], text: Callable | None = None, batch_size=32, workers=1) -> "EmbeddingsModel":
        """Fit the model to the documents.

        :param docs: List of documents to fit the model to.
        :param text: Function to extract text from a document. If not given, use Doc.text() method.
        :param batch_size: Batch size for encoding.
        :param workers: Number of workers for multiprocessing.
        """
        log.info("get_embeddings.start", n_embeddings=len(docs))
        t = time.time()

        keys, embeddings = self._load()

        # Create texts for embeddings
        if text:
            # Use custom text function
            texts = [text(doc) for doc in docs]
        else:
            # Use text method from Doc object
            texts = [doc.text() for doc in docs]

        # Check cache for existing embeddings
        missing_texts = list(set(texts) - set(keys))

        log.info(
            "get_embeddings.encoding",
            n_embeddings=len(texts),
            in_cache=len(texts) - len(missing_texts),
        )

        # Encode missing texts
        if missing_texts:
            if workers > 1:
                # NOTE: I haven't figured out why... anyway, single process is fast enough for 200k indicators
                raise NotImplementedError("Multiprocessing is much slower than a single process")
                # Start the multiprocessing pool
                pool = self.model.start_multi_process_pool(target_devices=workers * [DEVICE])
                # Encode sentences using multiprocessing
                batch_embeddings = self.model.encode_multi_process(
                    missing_texts,
                    pool,
                    batch_size=batch_size,
                    # precision="float32"
                )
                # Close the multiprocessing pool
                self.model.stop_multi_process_pool(pool)
            else:
                # Use single process encoding
                batch_embeddings = self.model.encode(
                    missing_texts,
                    convert_to_tensor=True,
                    batch_size=batch_size,
                    show_progress_bar=True,
                    device=DEVICE,
                )

            # Convert batch_embeddings to torch tensor if necessary
            if not isinstance(batch_embeddings, torch.Tensor):
                batch_embeddings = torch.from_numpy(batch_embeddings)

            # Ensure batch_embeddings are on the right device
            batch_embeddings = batch_embeddings.to(DEVICE)

            # Extend keys and embeddings
            keys.extend(missing_texts)
            if embeddings is None:
                embeddings = batch_embeddings
            else:
                embeddings = torch.cat([embeddings, batch_embeddings])

            # Save back keys and embeddings
            self._save(keys, embeddings)

        # Create a mapping from keys to indices
        key_to_index = {key: idx for idx, key in enumerate(keys)}

        # Get requested embeddings in order
        indices = [key_to_index[text] for text in texts]
        req_embeddings = embeddings[indices]  # ty: ignore

        self.docs = docs
        self.embeddings = req_embeddings

        log.info("get_embeddings.end", t=time.time() - t)

        return self

    def calculate_similarity(self, input_string: str) -> list[float]:
        embeddings = self.embeddings

        # Encode the input string and the document texts.
        input_embedding = self.model.encode(input_string, convert_to_tensor=True, device=DEVICE)

        # Compute the cosine similarity between the input and each document.
        def _get_score(input_embedding, embeddings, typ="cosine"):
            if typ == "cosine":
                score = util.pytorch_cos_sim(embeddings, input_embedding)
                score = (score + 1) / 2
            elif typ == "euclidean":
                # distance = torch.cdist(embeddings, input_embedding)
                score = util.euclidean_sim(embeddings, input_embedding)  # ty: ignore
                score = 1 / (1 - score)  # Normalize to [0, 1]
            else:
                raise ValueError(f"Invalid similarity type: {typ}")

            return score.cpu().numpy()[:, 0]

        return list(_get_score(input_embedding, embeddings))

    def get_sorted_documents_by_similarity(self, input_string: str) -> list[TDoc]:
        """Ingests an input string and a list of documents, returning the list of documents sorted by their semantic similarity to the input string."""
        log.info("get_sorted_documents_by_similarity.start", n_docs=len(self.docs))
        t = time.time()

        _docs = self.docs

        # Calculate the similarity scores.
        similarities = self.calculate_similarity(input_string)

        # Attach the similarity scores to the documents.
        # NOTE: we are updating docs in place which is not ideal, but copying would
        #   slow it down
        for i, doc in enumerate(_docs):
            doc.similarity = similarities[i]

        # Sort the documents by descending similarity score.
        sorted_documents = sorted(_docs, key=lambda x: x.similarity, reverse=True)  # ty: ignore

        log.info("get_sorted_documents_by_similarity.end", t=time.time() - t)

        return sorted_documents
