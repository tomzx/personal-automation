#!/usr/bin/env python3
# /// script
# dependencies = [
#   "notion-client>=2.7.0,<3",
#   "python-dotenv>=1.2.0,<2",
#   "rich>=13.9.0,<14",
# ]
# ///
"""
Find duplicate URLs in a Notion data source.

This script:
1. Reads all entries from a Notion data source (database)
2. Extracts URLs from a specified property (default: "URL")
3. Identifies and reports entries with duplicate URLs
4. Optionally outputs results to CSV
5. Optionally deduplicates by preserving the oldest entry and copying the most recent read time and rating

Environment variables required:rat
- NOTION_TOKEN: Your Notion integration token
- NOTION_DATA_SOURCE_ID: The ID of the data source to process
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from typing import Optional, List, Dict

try:
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
    from rich.console import Console as RichConsole
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Progress = None
    RichConsole = None

try:
    from notion_client import Client
    from dotenv import load_dotenv
except ImportError:
    print("Error: Required packages not installed.", file=sys.stderr)
    print("Run with uv: uv run notion-find-duplicate-urls.py", file=sys.stderr)
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
        response = notion.search(
            filter={"property": "object", "value": "data_source"}
        )

        data_sources = response.get("results", [])

        if not data_sources:
            print("No data sources found. Make sure your integration has access to data sources.")
            return

        for ds in data_sources:
            ds_id = ds.get("id", "Unknown")

            title = "Untitled Data Source"
            title_prop = ds.get("title", [])
            if title_prop and len(title_prop) > 0:
                title = title_prop[0].get("plain_text", "Untitled Data Source")

            print(f"  {title}")
            print(f"    ID: {ds_id}")
            print()

    except Exception as e:
        print(f"Error listing data sources: {e}", file=sys.stderr)


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


def get_url_from_page(properties: dict, property_name: str) -> Optional[str]:
    """
    Extract URL from a page's properties.

    Args:
        properties: The properties dict from a Notion page
        property_name: The name of the URL property

    Returns:
        The URL string or None if not found/empty
    """
    prop = properties.get(property_name, {})
    prop_type = prop.get("type")

    if prop_type == "url":
        return prop.get("url")

    return None


def get_page_title(properties: dict) -> str:
    """
    Extract the title from a page's properties.

    Args:
        properties: The properties dict from a Notion page

    Returns:
        The title string or "Untitled" if not found
    """
    for prop_name, prop_value in properties.items():
        if prop_value.get("type") == "title":
            title_array = prop_value.get("title", [])
            if title_array:
                return title_array[0].get("plain_text", "Untitled")
    return "Untitled"


def get_created_time(page: dict) -> Optional[str]:
    """
    Extract the created time from a page.

    Args:
        page: The page object from Notion

    Returns:
        ISO format timestamp string or None if not found
    """
    return page.get("created_time")


def get_read_time(properties: dict, property_name: str) -> Optional[str]:
    """
    Extract the read time (date) from a page's properties.

    Args:
        properties: The properties dict from a Notion page
        property_name: The name of the read time property

    Returns:
        ISO format date string or None if not set
    """
    prop = properties.get(property_name, {})
    prop_type = prop.get("type")

    if prop_type == "date":
        date_data = prop.get("date")
        if date_data:
            return date_data.get("start")

    return None


def get_rating(properties: dict, property_name: str) -> Optional[str]:
    """
    Extract the rating (select) from a page's properties.

    Args:
        properties: The properties dict from a Notion page
        property_name: The name of the rating property

    Returns:
        The rating value or None if not set
    """
    prop = properties.get(property_name, {})
    prop_type = prop.get("type")

    if prop_type == "select":
        select_data = prop.get("select")
        if select_data:
            return select_data.get("name")

    return None


def update_read_time(notion: Client, page_id: str, date_value: str, property_name: str) -> None:
    """
    Update the read time property of a Notion page.

    Args:
        notion: Notion client instance
        page_id: The ID of the page to update
        date_value: ISO format date string
        property_name: The name of the read time property
    """
    notion.pages.update(
        page_id=page_id,
        properties={
            property_name: {
                "date": {
                    "start": date_value
                }
            }
        }
    )


def delete_page(notion: Client, page_id: str) -> None:
    """
    Delete (archive) a Notion page.

    Args:
        notion: Notion client instance
        page_id: The ID of the page to delete
    """
    notion.pages.update(
        page_id=page_id,
        archived=True
    )


def find_duplicates(
    notion: Client,
    data_source_id: str,
    url_property_name: str = "URL",
    read_time_property_name: Optional[str] = None,
    rating_property_name: Optional[str] = None,
    include_empty: bool = False
) -> Dict[str, List[Dict]]:
    """
    Find all pages with duplicate URLs in the data source.

    Args:
        notion: Notion client instance
        data_source_id: The ID of the data source to process
        url_property_name: The name of the URL property (default: "URL")
        read_time_property_name: The name of the read time property (optional)
        rating_property_name: The name of the rating property (optional)
        include_empty: Whether to include pages with empty URLs in the duplicate check

    Returns:
        Dict mapping URLs to lists of pages that have that URL
    """
    # First, get the data source to find the actual property name (case-insensitive)
    try:
        data_source = notion.data_sources.retrieve(data_source_id=data_source_id)
        ds_properties = data_source.get("properties", {})

        actual_property_name = find_property_name(ds_properties, url_property_name)

        if not actual_property_name:
            print(f"Error: Property '{url_property_name}' not found in data source.", file=sys.stderr)
            print(f"\nAvailable properties:", file=sys.stderr)
            for prop in ds_properties.keys():
                prop_type = ds_properties[prop].get("type", "unknown")
                print(f"  - {prop} (type: {prop_type})", file=sys.stderr)
            sys.exit(1)

        prop_type = ds_properties[actual_property_name].get("type")
        if prop_type != "url":
            print(f"Warning: Property '{actual_property_name}' is type '{prop_type}', not 'url'", file=sys.stderr)

        print(f"Using property: '{actual_property_name}'")
        url_property_name = actual_property_name

        # Handle read time property if specified
        actual_read_time_property_name = None
        if read_time_property_name:
            actual_read_time_property_name = find_property_name(ds_properties, read_time_property_name)
            if actual_read_time_property_name:
                print(f"Using read time property: '{actual_read_time_property_name}'")
            else:
                print(f"Warning: Read time property '{read_time_property_name}' not found", file=sys.stderr)
                actual_read_time_property_name = None
            read_time_property_name = actual_read_time_property_name

        # Handle rating property if specified
        actual_rating_property_name = None
        if rating_property_name:
            actual_rating_property_name = find_property_name(ds_properties, rating_property_name)
            if actual_rating_property_name:
                print(f"Using rating property: '{actual_rating_property_name}'")
            else:
                print(f"Warning: Rating property '{rating_property_name}' not found", file=sys.stderr)
                actual_rating_property_name = None
            rating_property_name = actual_rating_property_name

    except Exception as e:
        print(f"Error retrieving data source: {e}", file=sys.stderr)
        raise

    # Query all pages in the data source
    url_to_pages: Dict[str, List[Dict]] = defaultdict(list)
    has_more = True
    start_cursor = None
    total_pages = 0

    # Setup progress bar if rich is available
    if RICH_AVAILABLE:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TextColumn("•"),
            TextColumn("{task.fields[count]} pages"),
            transient=False,
            refresh_per_second=10,
        ) as progress:
            task = progress.add_task("Fetching pages...", count=0)

            while has_more:
                query_params = {
                    "data_source_id": data_source_id
                }

                if start_cursor:
                    query_params["start_cursor"] = start_cursor

                response = notion.data_sources.query(**query_params)

                results = response.get("results", [])

                for page in results:
                    page_id = page["id"]
                    properties = page.get("properties", {})

                    url = get_url_from_page(properties, url_property_name)

                    if not url:
                        if include_empty:
                            url = ""
                        else:
                            continue

                    title = get_page_title(properties)
                    created_time = get_created_time(page)

                    page_data = {
                        "id": page_id,
                        "title": title,
                        "url": url,
                        "created_time": created_time,
                    }

                    if read_time_property_name:
                        page_data["read_time"] = get_read_time(properties, read_time_property_name)

                    if rating_property_name:
                        page_data["rating"] = get_rating(properties, rating_property_name)

                    url_to_pages[url].append(page_data)

                    total_pages += 1
                    progress.update(task, count=total_pages, refresh=True)

                has_more = response.get("has_more", False)
                start_cursor = response.get("next_cursor")

        print(f"Scanned {total_pages} page(s)")
    else:
        # Fallback without progress bar
        print("Scanning pages...")

        while has_more:
            query_params = {
                "data_source_id": data_source_id
            }

            if start_cursor:
                query_params["start_cursor"] = start_cursor

            response = notion.data_sources.query(**query_params)

            results = response.get("results", [])

            for page in results:
                page_id = page["id"]
                properties = page.get("properties", {})

                url = get_url_from_page(properties, url_property_name)

                if not url:
                    if include_empty:
                        url = ""
                    else:
                        continue

                title = get_page_title(properties)
                created_time = get_created_time(page)

                page_data = {
                    "id": page_id,
                    "title": title,
                    "url": url,
                    "created_time": created_time,
                }

                if read_time_property_name:
                    page_data["read_time"] = get_read_time(properties, read_time_property_name)

                if rating_property_name:
                    page_data["rating"] = get_rating(properties, rating_property_name)

                url_to_pages[url].append(page_data)

                total_pages += 1

            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        print(f"Scanned {total_pages} page(s)")

    # Filter to only duplicates
    duplicates = {
        url: pages for url, pages in url_to_pages.items()
        if len(pages) > 1
    }

    return duplicates


def print_duplicates(duplicates: Dict[str, List[Dict]]) -> None:
    """
    Print duplicate URLs and their associated pages.

    Args:
        duplicates: Dict mapping URLs to lists of pages
    """
    if not duplicates:
        print("\nNo duplicate URLs found.")
        return

    print(f"\nFound {len(duplicates)} duplicate URL(s):\n")

    for url, pages in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"URL: {url or '(empty)'}")
        print(f"  Appears {len(pages)} time(s):")
        for page in pages:
            print(f"    - {page['title']} (ID: {page['id']})")
        print()


def export_duplicates_to_csv(duplicates: Dict[str, List[Dict]], output_path: str) -> None:
    """
    Export duplicate URLs to a CSV file.

    Args:
        duplicates: Dict mapping URLs to lists of pages
        output_path: Path to the output CSV file
    """
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Count", "Page IDs", "Page Titles"])

        for url, pages in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True):
            page_ids = "; ".join(page["id"] for page in pages)
            page_titles = "; ".join(page["title"] for page in pages)
            writer.writerow([url or "(empty)", len(pages), page_ids, page_titles])

    print(f"Exported results to: {output_path}")


def deduplicate_urls(
    notion: Client,
    duplicates: Dict[str, List[Dict]],
    read_time_property_name: Optional[str] = None,
    rating_property_name: Optional[str] = None,
    dry_run: bool = False
) -> int:
    """
    Deduplicate URLs by preserving the oldest entry and copying the most recent values.

    For each set of duplicates:
    1. Find the oldest entry by created_time
    2. Find the most recent read_time value (if property specified)
    3. Find any rating value (if property specified)
    4. Copy the read_time and rating to the oldest entry
    5. Delete all other duplicate entries

    Args:
        notion: Notion client instance
        duplicates: Dict mapping URLs to lists of pages
        read_time_property_name: The name of the read time property (optional)
        rating_property_name: The name of the rating property (optional)
        dry_run: If True, don't actually make changes

    Returns:
        Number of entries that would be/were deleted
    """
    total_deleted = 0
    duplicate_items = list(duplicates.items())

    def process_item(console_print: callable, url: str, pages: list) -> int:
        """Process a single duplicate URL and return count of deletions."""
        # Sort by created_time to find the oldest
        sorted_pages = sorted(pages, key=lambda p: p.get("created_time", ""))

        oldest_page = sorted_pages[0]
        pages_to_delete = sorted_pages[1:]

        # Find the most recent read time (non-empty, most recent if multiple)
        latest_read_time = None
        if read_time_property_name:
            for page in reversed(sorted_pages):
                if page.get("read_time"):
                    latest_read_time = page["read_time"]
                    break

        # Find any rating value (prioritize non-None values)
        rating_value = None
        if rating_property_name:
            for page in reversed(sorted_pages):
                if page.get("rating"):
                    rating_value = page["rating"]
                    break

        console_print(f"\nURL: {url or '(empty)'}")
        console_print(f"  Keeping: {oldest_page['title']} (ID: {oldest_page['id']}, Created: {oldest_page['created_time']})")

        # Show what values will be copied
        if read_time_property_name and latest_read_time:
            console_print(f"  Copying {read_time_property_name}: {latest_read_time}")
        if rating_property_name and rating_value:
            console_print(f"  Copying {rating_property_name}: {rating_value}")

        actions = []

        # Check if we need to update read time
        if read_time_property_name and latest_read_time:
            current_read_time = oldest_page.get("read_time")
            if current_read_time != latest_read_time:
                actions.append(f"set {read_time_property_name} to {latest_read_time}")

        # Check if we need to update rating
        if rating_property_name and rating_value:
            current_rating = oldest_page.get("rating")
            if current_rating != rating_value:
                actions.append(f"set {rating_property_name} to {rating_value}")

        # Build delete list
        delete_count = len(pages_to_delete)
        if delete_count > 0:
            actions.append(f"delete {delete_count} duplicate(s)")

        if not actions:
            console_print("  No actions needed")
            return 0

        for action in actions:
            console_print(f"  Will {action}")

        if dry_run:
            console_print("  (dry run - skipped)")
            return delete_count

        # Perform the updates and deletions
        try:
            # Update the oldest page with read time
            if read_time_property_name and latest_read_time:
                if oldest_page.get("read_time") != latest_read_time:
                    update_read_time(notion, oldest_page["id"], latest_read_time, read_time_property_name)
                    console_print(f"  Updated {read_time_property_name}")

            # Update the oldest page with rating
            if rating_property_name and rating_value:
                if oldest_page.get("rating") != rating_value:
                    notion.pages.update(
                        page_id=oldest_page["id"],
                        properties={
                            rating_property_name: {
                                "select": {
                                    "name": rating_value
                                }
                            }
                        }
                    )
                    console_print(f"  Updated {rating_property_name}")

            # Delete duplicates
            for page in pages_to_delete:
                delete_page(notion, page["id"])

            console_print(f"  Deleted {delete_count} duplicate(s)")
            return delete_count

        except Exception as e:
            console_print(f"  Error: {e}")
            return 0

    if RICH_AVAILABLE:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("•"),
            TextColumn("[cyan]{task.completed}/{task.total} URLs"),
            TextColumn("•"),
            TimeRemainingColumn(),
            transient=False,
        ) as progress:
            total = len(duplicate_items)
            task = progress.add_task("Deduplicating...", total=total)

            for url, pages in duplicate_items:
                def console_print(msg: str):
                    print(msg)

                deleted = process_item(console_print, url, pages)
                total_deleted += deleted
                progress.update(task, advance=1, refresh=True)
    else:
        # Fallback without progress bar
        for url, pages in duplicate_items:
            def console_print(msg: str):
                print(msg)

            deleted = process_item(console_print, url, pages)
            total_deleted += deleted

    return total_deleted


def main():
    parser = argparse.ArgumentParser(
        description="Find duplicate URLs in a Notion data source"
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
        "--url-property",
        default="URL",
        help="Name of the URL property (default: 'URL')"
    )
    parser.add_argument(
        "--read-time-property",
        default="Read time",
        help="Name of the read time property to preserve when deduplicating (default: 'Read time')"
    )
    parser.add_argument(
        "--rating-property",
        default="Rating",
        help="Name of the rating property to preserve when deduplicating (default: 'Rating')"
    )
    parser.add_argument(
        "--deduplicate",
        action="store_true",
        help="Deduplicate by keeping oldest entry and copying read time/rating, then delete duplicates"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Include pages with empty URLs in duplicate check"
    )
    parser.add_argument(
        "--export-csv",
        help="Export results to a CSV file at the specified path"
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

    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()

    token = args.token or os.getenv("NOTION_TOKEN")
    data_source_id = args.data_source_id or os.getenv("NOTION_DATA_SOURCE_ID")

    if not token:
        print("Error: NOTION_TOKEN not provided", file=sys.stderr)
        print("Set it via --token argument or NOTION_TOKEN environment variable", file=sys.stderr)
        sys.exit(1)

    notion = Client(auth=token)

    if args.list_data_sources:
        list_available_data_sources(notion)
        sys.exit(0)

    if not data_source_id:
        print("Error: NOTION_DATA_SOURCE_ID not provided", file=sys.stderr)
        print("Set it via --data-source-id argument or NOTION_DATA_SOURCE_ID environment variable", file=sys.stderr)
        sys.exit(1)

    if args.deduplicate and not args.dry_run:
        response = input("This will permanently delete duplicate entries. Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    try:
        duplicates = find_duplicates(
            notion,
            data_source_id,
            url_property_name=args.url_property,
            read_time_property_name=args.read_time_property,
            rating_property_name=args.rating_property,
            include_empty=args.include_empty
        )

        if not duplicates:
            print("\nNo duplicate URLs found.")
            return

        if args.deduplicate:
            deleted = deduplicate_urls(
                notion,
                duplicates,
                read_time_property_name=args.read_time_property,
                rating_property_name=args.rating_property,
                dry_run=args.dry_run
            )
            print(f"\nTotal duplicates to delete: {deleted}")
        else:
            print_duplicates(duplicates)

        if args.export_csv and duplicates:
            export_duplicates_to_csv(duplicates, args.export_csv)

    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)

        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ["data_source", "database", "not found", "invalid", "object_not_found"]):
            print("\nThe data source may not exist or the integration doesn't have access to it.", file=sys.stderr)
            list_available_data_sources(notion)

        sys.exit(1)


if __name__ == "__main__":
    main()
