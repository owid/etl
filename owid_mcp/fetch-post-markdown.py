#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "mysql-connector-python",
#     "python-dotenv",
# ]
# ///

import sys
import argparse
import os
import json
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv



def get_db_connection():
    """
    Get a MySQL database connection using environment variables or default values.
    Loads environment variables from .env file if present.

    Expected environment variables:
    - MYSQL_HOST (default: localhost)
    - MYSQL_PORT (default: 3306)
    - MYSQL_USER (default: root)
    - MYSQL_PASSWORD (default: empty)
    - MYSQL_DATABASE (default: owid)
    """
    # Load environment variables from .env file if it exists
    load_dotenv()

    config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': os.getenv('MYSQL_DATABASE', 'owid'),
        'charset': 'utf8mb4',
        'use_unicode': True,
        'autocommit': True
    }

    return mysql.connector.connect(**config)


def fetch_post_markdown(identifier: str, timeout: int = 30) -> tuple[bool, str | dict, int | None]:
    """
    Fetch markdown content for a post by slug or Google Doc ID from the MySQL database.

    Args:
        identifier (str): The post slug or Google Doc ID to fetch
        timeout (int): Request timeout in seconds (not used with direct connection)

    Returns:
        tuple: (success: bool, result: str | dict, status_code: int)
        If successful, result is a dict with 'slug', 'title', 'markdown'
        If failed, result is an error message string
    """
    connection = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Try to fetch by ID first, then by slug if that fails
        query = "SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE id = %s LIMIT 1"
        cursor.execute(query, (identifier,))
        result = cursor.fetchone()

        if result is None:
            # Try by slug if ID lookup failed
            query = "SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE slug = %s LIMIT 1"
            cursor.execute(query, (identifier,))
            result = cursor.fetchone()

        if result is None:
            return False, f"No post found with identifier: {identifier}", 0

        post_data = {
            'slug': result[0] if result[0] else '',
            'title': result[1] if result[1] else '',
            'markdown': result[2] if result[2] else ''
        }

        return True, post_data, 0

    except Error as e:
        return False, f"MySQL error: {e}", None
    except Exception as e:
        return False, f"Unexpected error: {e}", None
    finally:
        if connection and connection.is_connected():
            connection.close()


def main() -> None:
    """Main function to handle command line arguments and fetch post markdown."""
    parser = argparse.ArgumentParser(
        description='Fetches the markdown content for an Our World in Data post by slug or Google Doc ID directly from the MySQL database.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Options:
  identifier            Required: The post slug (e.g., "many-countries-are-leapfrogging-landlines-and-going-straight-to-mobile-phones") or Google Doc ID
  -t, --timeout         Request timeout in seconds (default: 30)
  -o, --output          Save content to a file instead of printing to stdout
  -v, --verbose         Enable detailed output including query type and content length
  --include-metadata    Include title and slug metadata (default is markdown only)
  --json                Output as JSON with all fields (slug, title, markdown)

Examples:
  # Fetch by slug
  uv run scripts/fetch-post-markdown.py poverty --output poverty.md

  # Fetch by Google Doc ID
  uv run scripts/fetch-post-markdown.py 1BxGqJY9sHdW8s4K2lL3N4s7g5F6H --verbose

  # Include title and slug metadata
  uv run scripts/fetch-post-markdown.py covid-vaccinations --include-metadata

  # Output as JSON
  uv run scripts/fetch-post-markdown.py poverty --json
        """
    )

    parser.add_argument(
        'identifier',
        help='The post slug or Google Doc ID to fetch'
    )

    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=30,
        help='Request timeout in seconds (default: 30)'
    )

    parser.add_argument(
        '-o', '--output',
        help='Output file to save the markdown content (optional)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )

    parser.add_argument(
        '--include-metadata',
        action='store_true',
        help='Include title and slug metadata (default is markdown only)'
    )

    parser.add_argument(
        '--json',
        action='store_true',
        help='Output as JSON with all fields (slug, title, markdown)'
    )

    args = parser.parse_args()

    if args.verbose:
        print(f"Fetching post: {args.identifier}")
        print(f"Database: {os.getenv('MYSQL_DATABASE', 'owid')} at {os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', '3306')}")
        print("Will try to fetch by ID first, then by slug if that fails")

    # Fetch the post
    success, result, status_code = fetch_post_markdown(args.identifier, args.timeout)

    if success:
        post_data = result
        if args.verbose:
            print(f"Success!")
            print(f"Title: {post_data['title']}")
            print(f"Markdown length: {len(post_data['markdown'])} characters")

        # Prepare content for output
        if args.json:
            content = json.dumps(post_data, indent=2, ensure_ascii=False)
        elif args.include_metadata:
            content = f"# {post_data['title']}\n\nSlug: {post_data['slug']}\n\n{post_data['markdown']}"
        else:
            content = post_data['markdown']

        # Output content
        if args.output:
            try:
                with open(args.output, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Content saved to: {args.output}")
            except IOError as e:
                print(f"Error saving to file: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            print(content)
    else:
        print(f"Error fetching post: {result}", file=sys.stderr)
        if status_code:
            print(f"Exit code: {status_code}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()