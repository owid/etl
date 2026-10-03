---
applyTo: "**"
---

# OWID ETL Copilot Instructions

This repository contains the ETL (Extract, Transform, Load) system for Our World in Data. It processes, versions, and maintains datasets from various sources.

## Custom instructions
- NEVER query MCP servers with custom python code, just ask the MCP server directly. If it is not running, let me know.

## Architecture
- We use a DAG-based workflow with YAML configuration files in the `dag/` directory
- Processing happens in channels: Snapshot → Meadow → Garden → Grapher
- Data processing steps are defined with URIs like `data://garden/happiness/2025-03-28/happiness`

## Development Guidelines
- Follow PEP 8 Python style guidelines with a line length of 120 characters
- Use type hints for all function definitions
- For large datasets, prefer pandas operations that work on entire dataframes over row-by-row operations
- Always handle country name harmonization using the ETL's built-in functions
- Document functions and classes with docstrings
- Add appropriate metadata to datasets according to our metadata schema
- Write unit tests for new functionality
- Imports should be placed at the top of the file
- Let the errors propagate without try-catch block, don't catch them and re-raise with a generic message

## Process for Creating ETL Steps
1. Create a snapshot of the raw data
2. Process to meadow format (clean CSV)
3. Harmonize to garden format (standardized entities and variables)
4. Create grapher configuration if needed

## Tools We Use
- We use `make` for common development tasks
- Run `make test` to run tests
- Run `make format` to format code
- Run `make etl` to process data
- We use `pytest` for testing

## File Structure Conventions
- Steps for building datasets are in the `etl/steps` directory
- Core ETL code is in the `etl/` directory
- Snapshots are stored in `snapshots/<producer>/<date>/` directories
- Python processing code for snapshots is in `.py` files with the same name
- DVC files (`.dvc`) track the large data files
- Metadata for datasets is in `.meta.yml` files

Use these guidelines when suggesting code for this project.
