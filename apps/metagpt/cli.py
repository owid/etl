import os
from typing import Any

import click
import openai
import structlog

from etl.paths import BASE_DIR

# Initialize logger
log = structlog.get_logger()

# Example of new metadata format
NEW_METADATA_EXAMPLE = (
    BASE_DIR / "snapshots" / "emissions" / "2023-11-23" / "national_contributions_annual_emissions.csv.dvc"
)

# Additional instructions or configurations
ADDITIONAL_INSTRUCTIONS = """
Metadata Field Guidelines:

1. attribution (string, optional)
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Ends with year of date_published in parenthesis.
   - No semicolons.

2. attribution_short (string, recommended)
   - Use if producer name is long; shorten the producer name in an informative way.
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Refers to producer or well-known data product, not year.
   - Use acronym if well-known.

3. citation_full (string, required)
   - Capital letter start.
   - Ends with period.
   - Includes year of publication.
   - Match producer's format with minor edits.
   - List multiple sources for compilations.

4. date_accessed (string, required)
   - Format: YYYY-MM-DD.
   - Reflects access date of current version.

5. date_published (string, required)
   - Format: YYYY-MM-DD or YYYY.
   - Reflects publication date of current version.

6. description (string, recommended)
   - Capital letter start and period end.
   - Avoid other metadata fields unless crucial.
   - Succinct description of data product.

7. description_snapshot (string, recommended)
   - Capital letter start and period end.
   - Define if data product and snapshot differ.
   - Describe snapshot specifically.

8. license (string, required)
   - Standard license names or producer's custom text.
   - CC BY 4.0 if unspecified, pending confirmation.

9. license.url (string, required if existing)
   - Complete URL to license on producer's site.
   - Avoid generic license pages.

10. producer (string, required)
    - Capital letter start, except names like 'van Haasteren'.
    - No ending period, except 'et al.'.
    - Exclude dates, semicolons, OWID references.

11. title (string, required)
    - Capital letter start.
    - No ending period.
    - Identify data product, not snapshot.

12. title_snapshot (string, required if different)
    - Capital letter start.
    - No ending period.
    - Use if snapshot differs from data product.

13. url_download (string, required if existing)
    - Direct download URL or S3 URI.

14. url_main (string, required)
    - URL to data product's main site.

15. version_producer (string or number, recommended if existing)
    - Use producer's version naming.
"""


# Main CLI command setup with Click
@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to the metadata file.")
def main(path_to_file: str):
    """Process and update metadata using GPT-based tool."""
    log.info("Starting metadata update process.")
    try:
        metadata = read_metadata_file(path_to_file)
        updated_metadata = generate_metadata_update(path_to_file, metadata)
        save_updated_metadata(path_to_file, updated_metadata)
        log.info("Metadata update process completed successfully.")
    except Exception as e:
        log.error("Metadata update process failed.", error=str(e))


def read_metadata_file(path_to_file: str) -> str:
    """Reads a metadata file and returns its content."""
    try:
        with open(path_to_file, "r") as file:
            return file.read()
    except IOError as e:
        log.error("Error reading file", file_path=path_to_file, error=str(e))
        raise


def generate_metadata_update(path_to_file: str, metadata: str) -> str:
    """Generates updated metadata using OpenAI GPT."""
    messages = create_system_prompt(path_to_file, metadata)
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            temperature=0,
            messages=messages,
        )
        return response["choices"][0]["message"]["content"]
    except openai.error as e:
        log.error("OpenAI API error", error=str(e))
        raise


def create_system_prompt(path_to_file: str, metadata: str) -> str:
    """Creates the system prompt for the GPT model based on file path."""

    if "snapshot" in path_to_file:
        # Load example of new metadata format
        new_metadata_file = read_metadata_file(NEW_METADATA_EXAMPLE)

        system_prompt = f"""
        You are given a metadata file with information about the sources of the data in the old format. Now, we've transitioned to a new format. Update the metadata file to the new format. The new metadata file is as follows:

        New metadata format:
        {new_metadata_file}

        Format your response and add any additional fields based on these additional instructions:
        {ADDITIONAL_INSTRUCTIONS}
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": metadata},
        ]
        return messages

    elif "grapher" in path_to_file:
        system_prompt = f"""
            You are given a metadata file. We need to fill out the fields that start with TBD. Some of these only the user can do so keep them as they are but could you help us with at least filling out the following for each indicator:

            - description_from_producer - do a web search based on other information and URLs in the metadata file to find a description of the variable.
            - description_key - based on a web search and if description exists come up with some key bullet points (in a sentence format) that would help someone interpret the indicator. Can you make sure that these are going to be useful for the public to understand the indicator? Expand on any acronyms or any terms that a layperson might not be familiar with. Each bullet point can be more than one sentence if necessary but don't make it too long.
            - title_public - come up with a succinct title for the indicator that will be presented to the public
            - if description_short is not filled out, use the description_key and a web search to come up with one sentence to describe the indicator.
            - in make title_public ensure only the first letter is capitalized and the rest are lowercase unless it's an acryonym (e.g. GDP)
            - ignore title_variant for now

            Now, can you try to infer the above based on the other information in the metadata file and by browsing the web?

            You can use any links in the metadata file to help you. Generate a new metadata file with these fields filled out. Remove decsiption field at the indicator level (not the origins level) from the new metadata as it will no longer be used.

            Don't include any additional responses/notes in your response beyond the existing field as this will be saved directly as a file.
            Metadata file:
            {metadata}

            """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": metadata},
        ]
        return messages

    elif "garden" in path_to_file:
        log.error("Prompt for editing metadata in the garden step is not available yet.", file_path=path_to_file)

    else:
        log.error("Invalid file path", file_path=path_to_file)


def save_updated_metadata(original_file: str, updated_metadata: str):
    """Saves the updated metadata to a file."""
    output_file_name = "updated_with_gpt_" + os.path.basename(original_file)
    output_file_path = os.path.join(os.path.dirname(original_file), output_file_name)
    with open(output_file_path, "w") as file:
        file.write(updated_metadata)


if __name__ == "__main__":
    main()
