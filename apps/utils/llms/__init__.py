from pathlib import Path

import yaml

from .costs import estimate_llm_cost, generate_pricing_text

# Surface relevant cost functions
__all__ = [
    "estimate_llm_cost",
    "generate_pricing_text",
]

CURRENT_DIR = Path(__file__).parent.parent
# Load model information
# Available models from https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
LLM_MODELS_PATH = CURRENT_DIR / "utils" / "llms" / "models.yml"
with open(LLM_MODELS_PATH, "r") as f:
    LLM_MODELS = yaml.safe_load(f)
# Get costs
MODELS_COST = {m["name"]: m["cost"] for m in LLM_MODELS["models"]}
