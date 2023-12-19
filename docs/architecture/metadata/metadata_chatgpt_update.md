# Metadata Update Tool

This Python script is used to update metadata files using OpenAI's GPT model. The script is designed to work with two types of metadata files: 'snap' and 'grapher'.

## Main Functionality

The main function `main()` is set up as a command-line interface (CLI) using the `click` library. It takes three arguments:

- `path-to-file`: The path to the metadata file that needs to be updated.
- `output-dir`: The directory where the updated metadata file should be saved. If not provided, the updated file will be saved in the same directory as the original file.
- `overwrite`: A flag that, if set to True, overwrites the original file with the updated metadata.

## Helper Functions

The script includes several helper functions:

- `read_metadata_file()`: Reads a metadata file and returns its content.
- `process_chat_completion()`: Processes the chat completion response from the GPT model.
- `generate_metadata_update()`: Generates the updated metadata using the GPT model. This function handles different types of metadata files ('snap' and 'grapher') differently.
- `create_system_prompt()`: Creates the system prompt for the GPT model based on the type of metadata file.
- `check_gpt_response_format()`: Checks if the GPT model's response is in the correct format.

## Usage

To use this script, run it from the command line with the required arguments. For example:

```bash
python cli.py --path-to-file /path/to/metadata/file --output-dir /path/to/output/directory --overwrite