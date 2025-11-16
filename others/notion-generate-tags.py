#!/usr/bin/env python3
# /// script
# dependencies = [
#   "notion-client>=2.7.0,<3",
#   "python-dotenv>=1.2.0,<2",
# ]
# ///
"""
Generate tags for Notion data source entries using Claude.

This script:
1. Reads entries from a Notion data source (database) where "tags" field is empty
2. Retrieves the page content for each entry
3. Calls Claude to generate relevant tags for the content
4. Updates the "tags" field with the generated tags

Environment variables required:
- NOTION_TOKEN: Your Notion integration token
- NOTION_DATA_SOURCE_ID: The ID of the data source to process
"""

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Tuple

try:
    from notion_client import Client
    from dotenv import load_dotenv
except ImportError:
    print("Error: Required packages not installed.", file=sys.stderr)
    print("Run with uv: uv run notion-generate-tags.py", file=sys.stderr)
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


def get_existing_tags(notion: Client, data_source_id: str, property_name: str) -> List[str]:
    """
    Get all existing tags from a data source's multi_select property.

    Args:
        notion: Notion client instance
        data_source_id: The ID of the data source
        property_name: The name of the tags property

    Returns:
        List of existing tag names
    """
    try:
        data_source = notion.data_sources.retrieve(data_source_id=data_source_id)
        ds_properties = data_source.get("properties", {})

        if property_name not in ds_properties:
            return []

        property_config = ds_properties[property_name]
        if property_config.get("type") != "multi_select":
            return []

        # Get the options from the multi_select property
        multi_select_config = property_config.get("multi_select", {})
        options = multi_select_config.get("options", [])

        # Extract tag names
        tags = [option.get("name") for option in options if option.get("name")]
        return sorted(tags)

    except Exception as e:
        print(f"Warning: Could not fetch existing tags: {e}", file=sys.stderr)
        return []


def generate_tags_with_claude(content: str, existing_tags: List[str], max_tags: int = 5) -> List[str]:
    """
    Generate tags for content using Claude CLI.

    Args:
        content: The content to generate tags for
        existing_tags: List of existing tags in the data source
        max_tags: Maximum number of tags to generate

    Returns:
        List of generated tags
    """
    # Build the prompt with existing tags context
    existing_tags_text = ""
    if existing_tags:
        existing_tags_text = f"\n\nExisting tags in the system (prefer using these when applicable):\n{', '.join(existing_tags)}"

    prompt = f"""Generate tags for the following content. Return only the tags as a comma-separated list, with no additional explanation or formatting. Generate between 3 and {max_tags} relevant tags.{existing_tags_text}

Content:
{content}
"""

    try:
        # Call Claude CLI
        result = subprocess.run(
            ["claude", "-p", "--model", "haiku", prompt],
            capture_output=True,
            text=True,
            check=True
        )

        # Parse the output - expecting comma-separated tags
        output = result.stdout.strip()

        # Remove any markdown formatting or extra text
        if output.startswith("Tags:"):
            output = output[5:].strip()

        # Split by comma and clean up each tag
        tags = [tag.strip() for tag in output.split(",") if tag.strip()]

        # Limit to max_tags
        return tags[:max_tags]

    except subprocess.CalledProcessError as e:
        print(f"Error calling Claude: {e}", file=sys.stderr)
        print(f"Claude stdout: {e.stdout}", file=sys.stderr)
        print(f"Claude stderr: {e.stderr}", file=sys.stderr)
        return []
    except FileNotFoundError:
        print("Error: 'claude' command not found. Make sure Claude CLI is installed and in your PATH.", file=sys.stderr)
        return []


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


def update_tags(notion: Client, page_id: str, tags: List[str], property_name: str = "tags") -> None:
    """
    Update the tags property of a Notion page.

    Args:
        notion: Notion client instance
        page_id: The ID of the page to update
        tags: List of tag names to set
        property_name: The name of the property to update (default: "tags")
    """
    # Convert tags to Notion multi_select format
    tag_objects = [{"name": tag} for tag in tags]

    notion.pages.update(
        page_id=page_id,
        properties={
            property_name: {
                "type": "multi_select",
                "multi_select": tag_objects
            }
        }
    )


def process_data_source(
    notion: Client,
    data_source_id: str,
    property_name: str = "tags",
    max_tags: int = 5,
    dry_run: bool = False,
    limit: Optional[int] = None,
    parallel: int = 1
) -> None:
    """
    Process all pages in a data source that have empty tags.

    Args:
        notion: Notion client instance
        data_source_id: The ID of the data source to process
        property_name: The name of the tags property (default: "tags")
        max_tags: Maximum number of tags to generate per page
        dry_run: If True, don't actually update the data source
        limit: Maximum number of pages to process (None for all)
        parallel: Number of parallel Claude API calls (default: 1)
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

        # Verify it's a multi_select property
        prop_type = ds_properties[actual_property_name].get("type")
        if prop_type != "multi_select":
            print(f"Error: Property '{actual_property_name}' is type '{prop_type}', expected 'multi_select'", file=sys.stderr)
            sys.exit(1)

        print(f"Using property: '{actual_property_name}'")
        property_name = actual_property_name

    except Exception as e:
        print(f"Error retrieving data source: {e}", file=sys.stderr)
        raise

    # Get existing tags from the data source
    print("Fetching existing tags...")
    existing_tags = get_existing_tags(notion, data_source_id, property_name)
    if existing_tags:
        print(f"Found {len(existing_tags)} existing tags: {', '.join(existing_tags[:10])}{'...' if len(existing_tags) > 10 else ''}")
    else:
        print("No existing tags found in data source")

    # Query data source for pages with empty tags
    has_more = True
    start_cursor = None
    pages_processed = 0

    # Collect all pages to process
    all_pages = []

    while has_more and (limit is None or len(all_pages) < limit):
        query_params = {
            "data_source_id": data_source_id,
            "filter": {
                "property": property_name,
                "multi_select": {
                    "is_empty": True
                }
            }
        }

        if start_cursor:
            query_params["start_cursor"] = start_cursor

        response = notion.data_sources.query(**query_params)

        results = response.get("results", [])

        for page in results:
            if limit is not None and len(all_pages) >= limit:
                break
            all_pages.append(page)

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    print(f"\nFound {len(all_pages)} page(s) to process")

    def process_page(page: dict) -> Tuple[str, str, Optional[List[str]], Optional[str]]:
        """
        Process a single page and return results.

        Returns:
            Tuple of (page_id, title, tags or None, error message or None)
        """
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

        try:
            # Get page content
            content = get_page_content(notion, page_id)

            if not content.strip():
                return (page_id, title, None, "No content found")

            # Generate tags using Claude
            tags = generate_tags_with_claude(content, existing_tags, max_tags)

            if not tags:
                return (page_id, title, None, "No tags generated")

            return (page_id, title, tags, None)

        except Exception as e:
            return (page_id, title, None, str(e))

    # Process pages in parallel
    if parallel > 1:
        print(f"Processing with {parallel} parallel workers...")
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            future_to_page = {executor.submit(process_page, page): page for page in all_pages}

            for future in as_completed(future_to_page):
                page_id, title, tags, error = future.result()

                print(f"\nProcessed: {title}")
                print(f"  Page ID: {page_id}")

                if error:
                    print(f"  ⚠ {error}")
                    continue

                print(f"  Generated tags: {', '.join(tags)}")

                # Update the page
                if not dry_run:
                    try:
                        update_tags(notion, page_id, tags, property_name)
                        print(f"  ✓ Updated")
                        pages_processed += 1
                    except Exception as e:
                        print(f"  ✗ Update error: {e}", file=sys.stderr)
                else:
                    print(f"  (dry run - not updated)")
                    pages_processed += 1
    else:
        # Sequential processing
        for page in all_pages:
            page_id, title, tags, error = process_page(page)

            print(f"\nProcessing: {title}")
            print(f"  Page ID: {page_id}")

            if error:
                print(f"  ⚠ {error}")
                continue

            print(f"  Generated tags: {', '.join(tags)}")

            # Update the page
            if not dry_run:
                try:
                    update_tags(notion, page_id, tags, property_name)
                    print(f"  ✓ Updated")
                    pages_processed += 1
                except Exception as e:
                    print(f"  ✗ Update error: {e}", file=sys.stderr)
            else:
                print(f"  (dry run - not updated)")
                pages_processed += 1

    print(f"\nProcessed {pages_processed} page(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate tags for Notion data source entries using Claude"
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
        default="tags",
        help="Name of the tags property (default: 'tags')"
    )
    parser.add_argument(
        "--max-tags",
        type=int,
        default=5,
        help="Maximum number of tags to generate per page (default: 5)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of pages to process (default: process all)"
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel Claude API calls (default: 1)"
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

    # Initialize Notion client
    notion = Client(auth=token)

    # If just listing data sources, do that and exit
    if args.list_data_sources:
        list_available_data_sources(notion)
        sys.exit(0)

    if not data_source_id:
        print("Error: NOTION_DATA_SOURCE_ID not provided", file=sys.stderr)
        print("Set it via --data-source-id argument or NOTION_DATA_SOURCE_ID environment variable", file=sys.stderr)
        sys.exit(1)

    # Process the data source
    try:
        process_data_source(
            notion,
            data_source_id,
            property_name=args.property_name,
            max_tags=args.max_tags,
            dry_run=args.dry_run,
            limit=args.limit,
            parallel=args.parallel
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
