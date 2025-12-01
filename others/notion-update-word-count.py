#!/usr/bin/env python3
# /// script
# dependencies = [
#   "notion-client>=2.7.0,<3",
#   "python-dotenv>=1.2.0,<2",
# ]
# ///
"""
Update word counts for Notion data source entries.

This script:
1. Reads entries from a Notion data source (database) where "word count" field is empty
2. Retrieves the page content for each entry
3. Counts the number of words in the content
4. Updates the "word count" field with the calculated value

Environment variables required:
- NOTION_TOKEN: Your Notion integration token
- NOTION_DATA_SOURCE_ID: The ID of the data source to process
"""

import argparse
import os
import re
import sys
from typing import Optional

try:
    from notion_client import Client
    from dotenv import load_dotenv
except ImportError:
    print("Error: Required packages not installed.", file=sys.stderr)
    print("Run with uv: uv run notion-update-word-count.py", file=sys.stderr)
    sys.exit(1)


def list_available_data_sources(notion: Client) -> None:
    """
    List all data sources (databases) accessible to the integration.

    Args:
        notion: Notion client instance
    """
    print("\nAvailable data sources:")
    print("-" * 80)

    try:
        # Search for all data sources
        response = notion.search(
            filter={"property": "object", "value": "data_source"}
        )

        data_sources = response.get("results", [])

        if not data_sources:
            print("No data sources found. Make sure your integration has access to data sources.")
            return

        for ds in data_sources:
            ds_id = ds.get("id", "Unknown")

            # Get data source title
            title = "Untitled Data Source"
            title_prop = ds.get("title", [])
            if title_prop and len(title_prop) > 0:
                title = title_prop[0].get("plain_text", "Untitled Data Source")

            print(f"  {title}")
            print(f"    ID: {ds_id}")
            print()

    except Exception as e:
        print(f"Error listing data sources: {e}", file=sys.stderr)


def count_words(text: str) -> int:
    """
    Count words in text, ignoring formatting and special characters.

    Args:
        text: The text to count words in

    Returns:
        The number of words
    """
    # Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

    # Remove markdown formatting characters but keep the text
    text = re.sub(r'[#*_`~\[\](){}]', '', text)

    # Split on whitespace and filter empty strings
    words = [word for word in text.split() if word.strip()]

    return len(words)


def extract_text_from_blocks(blocks: list) -> str:
    """
    Extract plain text from Notion blocks.

    Args:
        blocks: List of Notion block objects

    Returns:
        Concatenated text from all blocks
    """
    text_parts = []

    for block in blocks:
        block_type = block.get("type")

        if not block_type:
            continue

        block_data = block.get(block_type, {})

        # Extract text from rich_text array
        if "rich_text" in block_data:
            for text_obj in block_data["rich_text"]:
                if "plain_text" in text_obj:
                    text_parts.append(text_obj["plain_text"])

        # Handle specific block types that might have text elsewhere
        if block_type == "code" and "rich_text" in block_data:
            for text_obj in block_data["rich_text"]:
                if "plain_text" in text_obj:
                    text_parts.append(text_obj["plain_text"])

        # Handle child blocks recursively if they exist
        if block.get("has_children"):
            # Note: Would need to fetch children separately in practice
            pass

    return " ".join(text_parts)


def get_page_content(notion: Client, page_id: str) -> str:
    """
    Retrieve all content from a Notion page.

    Args:
        notion: Notion client instance
        page_id: The ID of the page to retrieve

    Returns:
        The concatenated text content of the page
    """
    text_parts = []

    # Get all blocks from the page
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            response = notion.blocks.children.list(
                block_id=page_id,
                start_cursor=start_cursor
            )
        else:
            response = notion.blocks.children.list(block_id=page_id)

        blocks = response.get("results", [])
        text_parts.append(extract_text_from_blocks(blocks))

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    return " ".join(text_parts)


def get_property_value(properties: dict, property_name: str) -> Optional[int]:
    """
    Get the value of a number property from a page's properties.

    Args:
        properties: The properties dict from a Notion page
        property_name: The name of the property to retrieve

    Returns:
        The number value or None if empty/not found
    """
    prop = properties.get(property_name, {})
    prop_type = prop.get("type")

    if prop_type == "number":
        return prop.get("number")

    return None


def find_property_name(properties: dict, target_name: str) -> Optional[str]:
    """
    Find a property name in a case-insensitive manner.

    Args:
        properties: The properties dict from a Notion page or database
        target_name: The name to search for (case-insensitive)

    Returns:
        The actual property name if found, None otherwise
    """
    target_lower = target_name.lower()
    for prop_name in properties.keys():
        if prop_name.lower() == target_lower:
            return prop_name
    return None


def update_word_count(notion: Client, page_id: str, word_count: int, property_name: str = "word count") -> None:
    """
    Update the word count property of a Notion page.

    Args:
        notion: Notion client instance
        page_id: The ID of the page to update
        word_count: The word count value to set
        property_name: The name of the property to update (default: "word count")
    """
    notion.pages.update(
        page_id=page_id,
        properties={
            property_name: {
                "type": "number",
                "number": word_count
            }
        }
    )


def process_data_source(
    notion: Client,
    data_source_id: str,
    property_name: str = "word count",
    dry_run: bool = False
) -> None:
    """
    Process all pages in a data source that have empty word count.

    Args:
        notion: Notion client instance
        data_source_id: The ID of the data source to process
        property_name: The name of the word count property (default: "word count")
        dry_run: If True, don't actually update the data source
    """
    # First, get the data source to find the actual property name (case-insensitive)
    try:
        data_source = notion.data_sources.retrieve(data_source_id=data_source_id)
        ds_properties = data_source.get("properties", {})

        # Find the actual property name (case-insensitive)
        actual_property_name = find_property_name(ds_properties, property_name)

        if not actual_property_name:
            print(f"Error: Property '{property_name}' not found in data source.", file=sys.stderr)
            print(f"\nAvailable properties:", file=sys.stderr)
            for prop in ds_properties.keys():
                prop_type = ds_properties[prop].get("type", "unknown")
                print(f"  - {prop} (type: {prop_type})", file=sys.stderr)
            sys.exit(1)

        print(f"Using property: '{actual_property_name}'")
        property_name = actual_property_name

    except Exception as e:
        print(f"Error retrieving data source: {e}", file=sys.stderr)
        raise

    # Query data source for pages with empty word count
    has_more = True
    start_cursor = None
    pages_processed = 0

    while has_more:
        query_params = {
            "data_source_id": data_source_id,
            "filter": {
                "property": property_name,
                "number": {
                    "is_empty": True
                }
            }
        }

        if start_cursor:
            query_params["start_cursor"] = start_cursor

        response = notion.data_sources.query(**query_params)

        results = response.get("results", [])

        for page in results:
            page_id = page["id"]

            # Get page title if available
            title = "Untitled"
            properties = page.get("properties", {})
            for prop_name, prop_value in properties.items():
                if prop_value.get("type") == "title":
                    title_array = prop_value.get("title", [])
                    if title_array:
                        title = title_array[0].get("plain_text", "Untitled")
                    break

            print(f"Processing: {title} ({page_id})")

            try:
                # Get page content
                content = get_page_content(notion, page_id)

                # Count words
                word_count = count_words(content)

                print(f"  Word count: {word_count}")

                # Update the page
                if not dry_run:
                    update_word_count(notion, page_id, word_count, property_name)
                    print(f"  ✓ Updated")
                else:
                    print(f"  (dry run - not updated)")

                pages_processed += 1

            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                continue

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    print(f"\nProcessed {pages_processed} page(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Update word counts in a Notion data source"
    )
    parser.add_argument(
        "--data-source-id",
        help="Notion data source ID (or set NOTION_DATA_SOURCE_ID env var)"
    )
    parser.add_argument(
        "--token",
        help="Notion integration token (or set NOTION_TOKEN env var)"
    )
    parser.add_argument(
        "--property-name",
        default="word count",
        help="Name of the word count property (default: 'word count')"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--env-file",
        help="Path to .env file (optional)"
    )
    parser.add_argument(
        "--list-data-sources",
        action="store_true",
        help="List all available data sources and exit"
    )

    args = parser.parse_args()

    # Load environment variables from .env file if specified
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()

    # Get configuration from args or environment
    token = args.token or os.getenv("NOTION_TOKEN")
    data_source_id = args.data_source_id or os.getenv("NOTION_DATA_SOURCE_ID")

    if not token:
        print("Error: NOTION_TOKEN not provided", file=sys.stderr)
        print("Set it via --token argument or NOTION_TOKEN environment variable", file=sys.stderr)
        sys.exit(1)

    if not data_source_id:
        print("Error: NOTION_DATA_SOURCE_ID not provided", file=sys.stderr)
        print("Set it via --data-source-id argument or NOTION_DATA_SOURCE_ID environment variable", file=sys.stderr)
        sys.exit(1)

    # Initialize Notion client
    notion = Client(auth=token)

    # If just listing data sources, do that and exit
    if args.list_data_sources:
        list_available_data_sources(notion)
        sys.exit(0)

    # Process the data source
    try:
        process_data_source(
            notion,
            data_source_id,
            property_name=args.property_name,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)

        # If it looks like a data source error, show available data sources
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ["data_source", "database", "not found", "invalid", "object_not_found"]):
            print("\nThe data source may not exist or the integration doesn't have access to it.", file=sys.stderr)
            list_available_data_sources(notion)

        sys.exit(1)


if __name__ == "__main__":
    main()
