# Meta Update Tool

This tool, accessible via the command `etlcli metadata upgrader`, is designed to update metadata files using OpenAI's GPT model. It currently supports two types of metadata files: 'snapshot' and 'grapher'.

For 'grapher' metadata files, the tool completes the `description_from_producer`, `description_key`, and `description_short` fields, thereby enriching the metadata with more detailed information.

For 'snapshot' metadata files, the tool updates old, out-of-date files to a new format that is compatible with datapages.

## Usage
The main function is set up as a command-line interface (CLI). It takes three arguments:

- `path-to-file`: The path to the metadata file that needs to be updated.
- `output-dir`: The directory where the updated metadata file should be saved. If not provided, the updated file will be saved in the same directory as the original file.
- `overwrite`: A flag that, if set to True, overwrites the original file with the updated metadata.

!!! example
    To use this tool, run it from the command line with the required arguments. For example:

    ```bash
    etlcli metadata-upgrader --path-to-file /path/to/metadata/file --output-dir /path/to/output/directory --overwrite
    ```


## Snapshot vs Grapher Metadata Files

The `etlcli metadata-upgrader` tool handles 'snapshot' and 'grapher' metadata files differently.

### Snapshot Updates

- The GPT model is used to read a metadata file and generate a new metadata file that is compatible with the datapages.
- The new metadata file is structured identically to a hardcoded example file (`NEW_METADATA_EXAMPLE`).
- The GPT model is given a system prompt that includes the old metadata file, the new metadata format, and additional instructions for formatting the response.


### Grapher Updates

- Grapher updates are for metadata files related to the grapher step.
- The GPT model is used to fill out specific fields for each variable in the metadata file.
- The fields include 'description_from_producer', 'description_key', and 'description_short'.
- The GPT model is given a system prompt that includes the old metadata and instructions for filling out the fields.


## Setting Up OpenAI API Key

The `etlcli metadata-upgrader` tool requires the `OPENAI_API_KEY` environment variable to function properly. This is the API key provided by OpenAI for accessing their GPT model.

To obtain the API key, you need to sign up on the [OpenAI platform](https://openai.com). After signing up, you can find your API key in the account [settings](https://platform.openai.com/api-keys). For more information see OpenAI [FAQs](https://help.openai.com/en/articles/4936850-where-do-i-find-my-api-key)

Once you have the API key, set it as an environment variable in your development environment. For example, in a Unix-based system, you can add the following line to your `.bashrc` or `.zshrc`:

```bash
export OPENAI_API_KEY='your-api-key'
```
