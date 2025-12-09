from pathlib import Path

import yaml

CURRENT_DIR = Path(__file__).parent
# Load model information
# Available models from https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
LLM_MODELS_PATH = CURRENT_DIR / "models.yml"
with open(LLM_MODELS_PATH, "r") as f:
    LLM_MODELS = yaml.safe_load(f)
