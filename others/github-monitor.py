"""
Monitor GitHub issues and pull requests and publish events to NATS.

This script:
1. Fetches all open issues and PRs from tracked repositories using gh CLI
2. Scans for .active files to determine which issues/PRs are actively tracked
3. For each tracked issue/PR, checks if it's still open on GitHub
4. Monitors comments on issues and pull requests
5. Publishes events to NATS for:
   - New open issues/PRs discovered
   - Active issues/PRs that are still open (for processing)
   - Active issues/PRs that have been closed (to mark inactive)
   - New comments on issues
   - New comments on pull requests

Events published:
- github.issue.new: When a new open issue is discovered
- github.issue.updated: When an active issue needs processing
- github.issue.closed: When an active issue has been closed
- github.pr.new: When a new open PR is discovered
- github.pr.updated: When an active PR needs processing
- github.pr.closed: When an active PR has been closed
- github.issue.comment.new: When a new comment is added to an issue
- github.pr.comment.new: When a new comment is added to a pull request
"""

# /// script
# dependencies = [
#   "nats-py>=2.1.0,<3",
# ]
# ///

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from nats.aio.client import Client as NATS
    from nats.js.api import StreamConfig, RetentionPolicy, DiscardPolicy
    import asyncio
except ImportError:
    print("Error: nats-py is not installed. Install it with: pip install nats-py", file=sys.stderr)
    sys.exit(1)


def run_command(cmd: list[str], capture_output: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=True
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(cmd)}: {e}", file=sys.stderr)
        if e.stderr:
            print(f"stderr: {e.stderr}", file=sys.stderr)
        raise


def check_gh_installed() -> bool:
    """Check if gh CLI is installed."""
    try:
        subprocess.run(["gh", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def parse_duration(duration_str: str) -> int:
    """
    Parse a duration string in the format AdBhCmDs (e.g., 5m, 1h30m, 2d).

    Args:
        duration_str: Duration string with format like "5m", "1h30m", "2d12h", etc.
                     Supports: d (days), h (hours), m (minutes), s (seconds)

    Returns:
        Total duration in seconds

    Raises:
        ValueError: If the format is invalid
    """
    import re

    if not duration_str:
        raise ValueError("Duration string cannot be empty")

    # Pattern to match duration components
    pattern = r'(\d+)([dhms])'
    matches = re.findall(pattern, duration_str.lower())

    if not matches:
        raise ValueError(f"Invalid duration format: {duration_str}. Expected format like '5m', '1h30m', '2d', etc.")

    # Check if the entire string was consumed
    reconstructed = ''.join(f"{num}{unit}" for num, unit in matches)
    if reconstructed != duration_str.lower():
        raise ValueError(f"Invalid duration format: {duration_str}. Expected format like '5m', '1h30m', '2d', etc.")

    total_seconds = 0
    units = {
        'd': 86400,  # days
        'h': 3600,   # hours
        'm': 60,     # minutes
        's': 1       # seconds
    }

    for value, unit in matches:
        total_seconds += int(value) * units[unit]

    return total_seconds


def find_active_issues(base_path: Path, active_only: bool = True, repositories: Optional[list[str]] = None) -> list[tuple[str, str]]:
    """
    Find all active issues and PRs by scanning directories.
    Directory structure is assumed to be {base_path}/{owner/repo}/{issue_or_pr_number}.

    Args:
        base_path: Base path containing repository directories
        active_only: If True, only return issues/PRs with .active files. If False, return all.
        repositories: Optional list of repositories to filter by (format: owner/repo). If None, scan all.

    Returns:
        List of tuples: (repository in owner/repo format, issue_or_pr_number)
    """
    active_issues = []

    if not base_path.exists():
        print(f"Error: Path {base_path} does not exist", file=sys.stderr)
        return active_issues

    # Scan for repository directories (owner level)
    for owner_dir in base_path.iterdir():
        if not owner_dir.is_dir():
            continue

        owner = owner_dir.name

        # Scan for repo directories
        for repo_dir in owner_dir.iterdir():
            if not repo_dir.is_dir():
                continue

            repo_name = repo_dir.name
            repository = f"{owner}/{repo_name}"

            # Skip if repositories filter is provided and this repo is not in it
            if repositories is not None and repository not in repositories:
                continue

            # Scan for issue number directories
            for issue_dir in repo_dir.iterdir():
                if not issue_dir.is_dir():
                    continue

                issue_number = issue_dir.name

                # Check if this issue should be included
                if active_only:
                    active_file = issue_dir / ".active"
                    if active_file.exists():
                        active_issues.append((repository, issue_number))
                else:
                    # Include all issue directories
                    active_issues.append((repository, issue_number))

    return active_issues


def _build_issue_query(owner: str, repo: str, filter_clause: str, cursor: Optional[str] = None) -> str:
    """Build GraphQL query for fetching issues."""
    after_clause = f', after: "{cursor}"' if cursor else ""
    return f"""
    {{
      repository(owner: "{owner}", name: "{repo}") {{
        issues(first: 100, states: OPEN{after_clause}{filter_clause}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            number
            title
            body
            url
            state
            createdAt
            updatedAt
            closedAt
            author {{
              login
            }}
            assignees(first: 10) {{
              nodes {{
                login
              }}
            }}
            labels(first: 10) {{
              nodes {{
                name
              }}
            }}
          }}
        }}
      }}
    }}
    """


def _build_pr_query(owner: str, repo: str, filter_clause: str, cursor: Optional[str] = None) -> str:
    """Build GraphQL query for fetching pull requests."""
    after_clause = f', after: "{cursor}"' if cursor else ""
    return f"""
    {{
      repository(owner: "{owner}", name: "{repo}") {{
        pullRequests(first: 100, states: OPEN{after_clause}{filter_clause}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            number
            title
            body
            url
            state
            createdAt
            updatedAt
            closedAt
            mergedAt
            author {{
              login
            }}
            assignees(first: 10) {{
              nodes {{
                login
              }}
            }}
            labels(first: 10) {{
              nodes {{
                name
              }}
            }}
            isDraft
            mergeable
            reviewDecision
          }}
        }}
      }}
    }}
    """


def _parse_issue_node(issue: dict) -> dict:
    """Parse a GraphQL issue node into standardized format."""
    return {
        "type": "issue",
        "number": issue["number"],
        "title": issue["title"],
        "body": issue["body"],
        "url": issue["url"],
        "state": issue["state"],
        "created_at": issue["createdAt"],
        "updated_at": issue["updatedAt"],
        "closed_at": issue["closedAt"],
        "author": issue["author"]["login"] if issue["author"] else "ghost",
        "assignees": [a["login"] for a in issue["assignees"]["nodes"]],
        "labels": [l["name"] for l in issue["labels"]["nodes"]]
    }


def _parse_pr_node(pr: dict) -> dict:
    """Parse a GraphQL pull request node into standardized format."""
    return {
        "type": "pr",
        "number": pr["number"],
        "title": pr["title"],
        "body": pr["body"],
        "url": pr["url"],
        "state": pr["state"],
        "created_at": pr["createdAt"],
        "updated_at": pr["updatedAt"],
        "closed_at": pr["closedAt"],
        "merged_at": pr["mergedAt"],
        "author": pr["author"]["login"] if pr["author"] else "ghost",
        "assignees": [a["login"] for a in pr["assignees"]["nodes"]],
        "labels": [l["name"] for l in pr["labels"]["nodes"]],
        "is_draft": pr["isDraft"],
        "mergeable": pr["mergeable"],
        "review_decision": pr["reviewDecision"]
    }


def _fetch_paginated_items(
    repository: str,
    query_builder: callable,
    data_path: list[str],
    parser: callable
) -> dict[str, dict]:
    """
    Generic function to fetch paginated items from GitHub GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        query_builder: Function that builds the GraphQL query with (owner, repo, cursor) params
        data_path: Path to the data in the response (e.g., ["repository", "issues"])
        parser: Function to parse each node

    Returns:
        Dictionary mapping item numbers to parsed item data
    """
    items = {}
    has_next_page = True
    end_cursor = None

    while has_next_page:
        try:
            result = run_command([
                "gh", "api", "graphql",
                "-f", f"query={query_builder(end_cursor)}"
            ])

            data = json.loads(result.stdout)

            # Navigate to the data using the path
            current = data.get("data", {})
            for key in data_path:
                if current is None:
                    print(f"Error: Invalid response from GraphQL API for {repository}", file=sys.stderr)
                    return items
                current = current.get(key)

            if current is None:
                print(f"Error: Invalid response from GraphQL API for {repository}", file=sys.stderr)
                break

            # Process nodes
            for node in current["nodes"]:
                item_number = str(node["number"])
                items[item_number] = parser(node)

            # Check pagination
            has_next_page = current["pageInfo"]["hasNextPage"]
            end_cursor = current["pageInfo"]["endCursor"]

        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error fetching items for {repository}: {e}", file=sys.stderr)
            break

    return items


def get_open_issues(repository: str, updated_since: Optional[str] = None, item_type: Optional[str] = None) -> dict[str, dict]:
    """
    Get the list of open issues and/or pull requests from GitHub using GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        updated_since: Optional ISO8601 timestamp to filter issues/PRs updated since this time
        item_type: Optional type filter - "issue", "pr", or None for both

    Returns:
        Dictionary mapping issue/PR numbers (as strings) to data dictionaries.
        Each entry includes a "type" field set to either "issue" or "pr".
    """
    try:
        owner, repo = repository.split("/")

        # Build the filter clause
        filter_clause = ""
        if updated_since:
            filter_clause = f', filterBy: {{since: "{updated_since}"}}'

        items = {}

        # Fetch issues if requested
        if item_type is None or item_type == "issue":
            def issue_query_builder(cursor):
                return _build_issue_query(owner, repo, filter_clause, cursor)

            issue_items = _fetch_paginated_items(
                repository,
                issue_query_builder,
                ["repository", "issues"],
                _parse_issue_node
            )
            items.update(issue_items)

        # Fetch pull requests if requested
        if item_type is None or item_type == "pr":
            def pr_query_builder(cursor):
                return _build_pr_query(owner, repo, filter_clause, cursor)

            pr_items = _fetch_paginated_items(
                repository,
                pr_query_builder,
                ["repository", "pullRequests"],
                _parse_pr_node
            )
            items.update(pr_items)

        return items
    except (ValueError) as e:
        print(f"Error getting open issues for {repository}: {e}", file=sys.stderr)
        return {}


def get_tracked_repositories(base_path: Path) -> list[str]:
    """
    Get list of repositories that are being tracked.
    Looks for existing repository directories in base_path.
    Directory structure is {base_path}/{owner}/{repo}.

    Returns:
        List of repository names in "owner/repo" format
    """
    repositories = []

    if not base_path.exists():
        return repositories

    # Scan for owner directories
    for owner_dir in base_path.iterdir():
        if owner_dir.is_dir():
            owner = owner_dir.name

            # Scan for repo directories under owner
            for repo_dir in owner_dir.iterdir():
                if repo_dir.is_dir():
                    repo_name = repo_dir.name
                    repositories.append(f"{owner}/{repo_name}")

    return repositories


def get_last_comment_check(base_path: Path, repository: str, issue_number: str, check_type: str = "issue") -> Optional[str]:
    """
    Get the timestamp of the last comment check for an issue or PR.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        issue_number: Issue or PR number
        check_type: Type of check ("issue" or "pr")

    Returns:
        ISO8601 timestamp string, or None if never checked
    """
    issue_dir = base_path / repository / issue_number
    timestamp_file = issue_dir / f".last_{check_type}_comment_check"

    if timestamp_file.exists():
        try:
            return timestamp_file.read_text().strip()
        except Exception as e:
            print(f"Error reading timestamp file {timestamp_file}: {e}", file=sys.stderr)
            return None

    return None


def save_last_comment_check(base_path: Path, repository: str, issue_number: str, timestamp: str, check_type: str = "issue") -> None:
    """
    Save the timestamp of the last comment check for an issue or PR.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        issue_number: Issue or PR number
        timestamp: ISO8601 timestamp string
        check_type: Type of check ("issue" or "pr")
    """
    issue_dir = base_path / repository / issue_number
    timestamp_file = issue_dir / f".last_{check_type}_comment_check"

    try:
        timestamp_file.write_text(timestamp)
    except Exception as e:
        print(f"Error writing timestamp file {timestamp_file}: {e}", file=sys.stderr)


def get_last_checked(base_path: Path, repository: str, issue_number: str) -> Optional[str]:
    """
    Get the timestamp of the last time an issue was monitored.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        issue_number: Issue or PR number

    Returns:
        ISO8601 timestamp string, or None if never checked
    """
    issue_dir = base_path / repository / issue_number
    timestamp_file = issue_dir / ".last_checked"

    if timestamp_file.exists():
        try:
            return timestamp_file.read_text().strip()
        except Exception as e:
            print(f"Error reading timestamp file {timestamp_file}: {e}", file=sys.stderr)
            return None

    return None


def save_last_checked(base_path: Path, repository: str, issue_number: str, timestamp: str) -> None:
    """
    Save the timestamp of the last time an issue was monitored.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        issue_number: Issue or PR number
        timestamp: ISO8601 timestamp string
    """
    issue_dir = base_path / repository / issue_number
    timestamp_file = issue_dir / ".last_checked"

    try:
        # Ensure the directory exists
        issue_dir.mkdir(parents=True, exist_ok=True)
        timestamp_file.write_text(timestamp)
    except Exception as e:
        print(f"Error writing last checked file {timestamp_file}: {e}", file=sys.stderr)


def _build_comment_query(owner: str, repo: str, number: str, item_type: str, cursor: Optional[str] = None) -> str:
    """Build GraphQL query for fetching comments on issues or PRs."""
    after_clause = f', after: "{cursor}"' if cursor else ""
    type_name = "issue" if item_type == "issue" else "pullRequest"

    return f"""
    {{
      repository(owner: "{owner}", name: "{repo}") {{
        {type_name}(number: {number}) {{
          comments(first: 100{after_clause}) {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            nodes {{
              id
              databaseId
              url
              author {{
                login
              }}
              authorAssociation
              body
              bodyText
              createdAt
              updatedAt
              publishedAt
              lastEditedAt
              isMinimized
              minimizedReason
              reactions(first: 10) {{
                totalCount
                nodes {{
                  content
                  user {{
                    login
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """


def _parse_comment_node(comment: dict) -> dict:
    """Parse a GraphQL comment node into standardized format."""
    return {
        "id": comment["id"],
        "database_id": comment["databaseId"],
        "url": comment["url"],
        "author": comment["author"]["login"] if comment["author"] else "ghost",
        "author_association": comment["authorAssociation"],
        "body": comment["body"],
        "body_text": comment["bodyText"],
        "created_at": comment["createdAt"],
        "updated_at": comment["updatedAt"],
        "published_at": comment["publishedAt"],
        "last_edited_at": comment["lastEditedAt"],
        "is_minimized": comment["isMinimized"],
        "minimized_reason": comment["minimizedReason"],
        "reactions": {
            "total_count": comment["reactions"]["totalCount"],
            "items": [
                {
                    "content": r["content"],
                    "user": r["user"]["login"] if r["user"] else "ghost"
                }
                for r in comment["reactions"]["nodes"]
            ]
        }
    }


def _fetch_paginated_comments(
    repository: str,
    number: str,
    item_type: str,
    updated_since: Optional[str] = None
) -> list[dict]:
    """
    Generic function to fetch paginated comments from GitHub GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        number: Issue or PR number
        item_type: Either "issue" or "pr"
        updated_since: Optional ISO8601 timestamp to filter comments

    Returns:
        List of parsed comment dictionaries
    """
    try:
        owner, repo = repository.split("/")
        comments = []
        has_next_page = True
        end_cursor = None

        while has_next_page:
            query = _build_comment_query(owner, repo, number, item_type, end_cursor)

            result = run_command([
                "gh", "api", "graphql",
                "-f", f"query={query}"
            ])

            data = json.loads(result.stdout)

            # Navigate to the correct data path
            type_key = "issue" if item_type == "issue" else "pullRequest"
            if "data" not in data or not data["data"]["repository"] or not data["data"]["repository"][type_key]:
                print(f"Error: Invalid response from GraphQL API for {repository}#{number}", file=sys.stderr)
                break

            item_data = data["data"]["repository"][type_key]
            comment_data = item_data["comments"]

            for comment in comment_data["nodes"]:
                # Filter by updated_since if provided
                if updated_since:
                    comment_updated = comment["updatedAt"]
                    if comment_updated <= updated_since:
                        continue

                comments.append(_parse_comment_node(comment))

            # Check pagination
            has_next_page = comment_data["pageInfo"]["hasNextPage"]
            end_cursor = comment_data["pageInfo"]["endCursor"]

        return comments
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Error getting comments for {repository}#{number}: {e}", file=sys.stderr)
        return []


def get_issue_comments(repository: str, issue_number: str, updated_since: Optional[str] = None) -> list[dict]:
    """
    Get comments on an issue from GitHub using GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        issue_number: Issue number
        updated_since: Optional ISO8601 timestamp to filter comments updated since this time

    Returns:
        List of comment dictionaries with all available fields from GraphQL
    """
    return _fetch_paginated_comments(repository, issue_number, "issue", updated_since)


def get_type_from_file(base_path: Path, repository: str, number: str) -> Optional[str]:
    """
    Get the type (issue or pr) from the .type file.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        number: Issue/PR number

    Returns:
        "issue" or "pr" if type file exists, None otherwise
    """
    issue_dir = base_path / repository / number
    type_file = issue_dir / ".type"

    if type_file.exists():
        try:
            content = type_file.read_text().strip().lower()
            if content in ("issue", "pr"):
                return content
        except Exception as e:
            print(f"Error reading type file {type_file}: {e}", file=sys.stderr)

    return None


def save_type_to_file(base_path: Path, repository: str, number: str, type_value: str) -> None:
    """
    Save the type (issue or pr) to the .type file.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        number: Issue/PR number
        type_value: Either "issue" or "pr"
    """
    issue_dir = base_path / repository / number
    type_file = issue_dir / ".type"

    try:
        # Ensure the directory exists
        issue_dir.mkdir(parents=True, exist_ok=True)
        type_file.write_text(type_value.lower())
    except Exception as e:
        print(f"Error writing type file {type_file}: {e}", file=sys.stderr)


def is_pull_request(repository: str, number: str, base_path: Optional[Path] = None) -> bool:
    """
    Check if a given number is a pull request.

    Args:
        repository: Repository in "owner/repo" format
        number: Issue/PR number
        base_path: Optional base path to check for cached .type file

    Returns:
        True if it's a PR, False otherwise
    """
    # First check if we have a cached type
    if base_path:
        cached_type = get_type_from_file(base_path, repository, number)
        if cached_type is not None:
            return cached_type == "pr"

    # If not cached, check via API
    try:
        # Try to get PR info - if it succeeds, it's a PR
        result = subprocess.run(
            ["gh", "pr", "view", number, "--repo", repository],
            capture_output=True,
            text=True
        )
        is_pr = result.returncode == 0

        # Cache the result if base_path is provided
        if base_path:
            save_type_to_file(base_path, repository, number, "pr" if is_pr else "issue")

        return is_pr
    except Exception as e:
        print(f"Error checking if {repository}#{number} is a PR: {e}", file=sys.stderr)
        return False


def get_pr_comments(repository: str, pr_number: str, updated_since: Optional[str] = None) -> list[dict]:
    """
    Get comments on a pull request from GitHub using GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        pr_number: PR number
        updated_since: Optional ISO8601 timestamp to filter comments updated since this time

    Returns:
        List of comment dictionaries with all available fields from GraphQL
    """
    return _fetch_paginated_comments(repository, pr_number, "pr", updated_since)


async def publish_event(js, subject: str, data: dict) -> None:
    """Publish an event to NATS JetStream."""
    try:
        message = json.dumps(data).encode()
        await js.publish(subject, message)
        print(f"  Published event to {subject}: {data}")
    except Exception as e:
        print(f"Error publishing event to {subject}: {e}", file=sys.stderr)


async def ensure_jetstream_stream(nc: NATS, stream_name: str = "GITHUB_EVENTS"):
    """
    Ensure JetStream stream exists for GitHub events.

    Args:
        nc: NATS client connection
        stream_name: Name of the stream to create (default: GITHUB_EVENTS)

    Returns:
        JetStream context
    """
    try:
        js = nc.jetstream()

        # Try to get stream info to check if it exists
        try:
            stream_info = await js.stream_info(stream_name)
            print(f"JetStream stream '{stream_name}' already exists")
        except Exception:
            # Stream doesn't exist, create it
            # Create the stream with appropriate configuration
            stream_config = StreamConfig(
                name=stream_name,
                subjects=["github.>"],  # All github.* subjects
                retention=RetentionPolicy.LIMITS,  # Keep messages based on limits
                discard=DiscardPolicy.OLD,  # Discard old messages when limits reached
                max_age=7 * 24 * 60 * 60,  # Keep for 7 days (in seconds)
                max_msgs=10000,  # Keep up to 10k messages
                max_bytes=100 * 1024 * 1024,  # Max 100MB storage
            )

            await js.add_stream(stream_config)
            print(f"Created JetStream stream '{stream_name}'")

        return js

    except Exception as e:
        print(f"Error ensuring JetStream stream: {e}", file=sys.stderr)
        raise


async def monitor_repositories(
    js,
    base_path: Path,
    repositories: list[str],
    dry_run: bool = False,
    updated_since: Optional[str] = None,
    item_type: Optional[str] = None
) -> int:
    """
    Monitor repositories and publish events for new open issues and/or PRs.

    Args:
        js: JetStream context
        base_path: Base path for issue directories
        repositories: List of repositories to monitor
        dry_run: If True, don't publish events
        updated_since: Optional ISO8601 timestamp to filter issues/PRs updated since this time
        item_type: Optional type filter - "issue", "pr", or None for both

    Returns:
        Number of new issues/PRs discovered
    """
    new_issues_count = 0
    current_time = datetime.now(timezone.utc).isoformat()

    type_label = "issues/PRs" if item_type is None else ("issues" if item_type == "issue" else "PRs")

    for repository in repositories:
        if updated_since:
            print(f"Fetching open {type_label} for {repository} updated since {updated_since}...")
        else:
            print(f"Fetching open {type_label} for {repository}...")

        open_items = get_open_issues(repository, updated_since, item_type)

        if not open_items:
            print(f"  No open {type_label} found or error fetching")
            continue

        print(f"  Found {len(open_items)} open {type_label}")

        for number, item_data in open_items.items():
            item_dir = base_path / repository / number
            item_type_from_data = item_data.get("type", "issue")

            if not item_dir.exists():
                if item_type_from_data == "pr":
                    print(f"    New PR discovered: {repository}#{number}")
                    event_subject = "github.pr.new"
                else:
                    print(f"    New issue discovered: {repository}#{number}")
                    event_subject = "github.issue.new"

                event_data = {
                    "repository": repository,
                    **item_data  # Include all item data from GraphQL
                }

                if dry_run:
                    print(f"    [DRY RUN] Would publish {event_subject} event")
                    print(f"    [DRY RUN] Would save .last_checked timestamp")
                    print(f"    [DRY RUN] Would save .type file as '{item_type_from_data}'")
                else:
                    await publish_event(js, event_subject, event_data)
                    save_last_checked(base_path, repository, number, current_time)
                    save_type_to_file(base_path, repository, number, item_type_from_data)
                    new_issues_count += 1

        print()

    return new_issues_count


async def process_active_issues(
    js,
    base_path: Path,
    active_issues: list[tuple[str, str]],
    dry_run: bool = False,
    item_type: Optional[str] = None
) -> None:
    """Process all active issues and/or PRs and publish appropriate events."""
    current_time = datetime.now(timezone.utc).isoformat()

    # Group issues by repository
    issues_by_repo: dict[str, list[str]] = {}
    for repository, issue_number in active_issues:
        if repository not in issues_by_repo:
            issues_by_repo[repository] = []
        issues_by_repo[repository].append(issue_number)

    type_label = "issues/PRs" if item_type is None else ("issues" if item_type == "issue" else "PRs")

    # Process each repository
    for repository, issue_numbers in issues_by_repo.items():
        print(f"Checking open {type_label} for {repository}...")
        open_items = get_open_issues(repository, item_type=item_type)

        if not open_items and len(issue_numbers) > 0:
            print(f"  Warning: Could not fetch open issues/PRs for {repository}")
            continue

        # Process each issue/PR for this repository
        for number in issue_numbers:
            print(f"  Processing {repository}#{number}...")

            if number in open_items:
                item_data = open_items[number]
                item_type_from_data = item_data.get("type", "issue")

                # Get the last check timestamp
                last_check = get_last_checked(base_path, repository, number)

                # Check if the item has been updated since the last check
                updated_at = item_data.get("updated_at")
                has_update = last_check is None or (updated_at and updated_at > last_check)

                event_data = {
                    "repository": repository,
                    "number": number,
                    **item_data  # Include all item data from GraphQL
                }

                if item_type_from_data == "pr":
                    if has_update:
                        print(f"    PR has been updated, emitting update event")
                        event_subject = "github.pr.updated"
                    else:
                        print(f"    PR is still open (no updates since last check)")
                        event_subject = None
                else:
                    if has_update:
                        print(f"    Issue has been updated, emitting update event")
                        event_subject = "github.issue.updated"
                    else:
                        print(f"    Issue is still open (no updates since last check)")
                        event_subject = None

                if dry_run:
                    if event_subject:
                        print(f"    [DRY RUN] Would publish {event_subject} event")
                    print(f"    [DRY RUN] Would save .last_checked timestamp")
                else:
                    if event_subject:
                        await publish_event(js, event_subject, event_data)
                    save_last_checked(base_path, repository, number, current_time)
            else:
                # For closed items, we don't have the data from GraphQL since it only returns open items
                # But we can still check the cached type and include basic info
                cached_type = get_type_from_file(base_path, repository, number)

                event_data = {
                    "repository": repository,
                    "number": number
                }

                if cached_type == "pr":
                    print(f"    PR is closed")
                    event_subject = "github.pr.closed"
                else:
                    print(f"    Issue is closed")
                    event_subject = "github.issue.closed"

                if dry_run:
                    print(f"    [DRY RUN] Would publish {event_subject} event")
                    print(f"    [DRY RUN] Would save .last_checked timestamp")
                else:
                    await publish_event(js, event_subject, event_data)
                    save_last_checked(base_path, repository, number, current_time)

        print()


def get_repository_last_comment_check(base_path: Path, repository: str, check_type: str = "issue") -> Optional[str]:
    """
    Get the earliest last comment check timestamp for all issues/PRs in a repository.
    This allows us to fetch all comments since the earliest check with a single query.

    Args:
        base_path: Base path for issue directories
        repository: Repository in "owner/repo" format
        check_type: Type of check ("issue" or "pr")

    Returns:
        ISO8601 timestamp string of the earliest last check, or None if never checked
    """
    repo_path = base_path / repository
    if not repo_path.exists():
        return None

    earliest_timestamp = None

    for issue_dir in repo_path.iterdir():
        if not issue_dir.is_dir():
            continue

        timestamp_file = issue_dir / f".last_{check_type}_comment_check"
        if timestamp_file.exists():
            try:
                timestamp = timestamp_file.read_text().strip()
                if earliest_timestamp is None or timestamp < earliest_timestamp:
                    earliest_timestamp = timestamp
            except Exception as e:
                print(f"Error reading timestamp file {timestamp_file}: {e}", file=sys.stderr)

    return earliest_timestamp


def get_all_repository_comments(repository: str, item_type: str, updated_since: Optional[str] = None) -> dict[str, list[dict]]:
    """
    Get all comments on issues or PRs in a repository using a single GraphQL query.

    Args:
        repository: Repository in "owner/repo" format
        item_type: Either "issue" or "pr"
        updated_since: Optional ISO8601 timestamp to filter comments updated since this time

    Returns:
        Dictionary mapping issue/PR numbers (as strings) to lists of comment dictionaries
    """
    try:
        owner, repo = repository.split("/")
        type_name = "issues" if item_type == "issue" else "pullRequests"
        singular_type = "issue" if item_type == "issue" else "pullRequest"

        # Build filter clause for updated_since (only issues support filterBy)
        filter_clause = ""
        if updated_since and item_type == "issue":
            filter_clause = f', filterBy: {{since: "{updated_since}"}}'

        has_next_page = True
        end_cursor = None
        comments_by_number: dict[str, list[dict]] = {}

        while has_next_page:
            after_clause = f', after: "{end_cursor}"' if end_cursor else ""

            # Query to get all open items with their comments
            query = f"""
            {{
              repository(owner: "{owner}", name: "{repo}") {{
                {type_name}(first: 100, states: OPEN{after_clause}{filter_clause}) {{
                  pageInfo {{
                    hasNextPage
                    endCursor
                  }}
                  nodes {{
                    number
                    comments(first: 100, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
                      pageInfo {{
                        hasNextPage
                        endCursor
                      }}
                      nodes {{
                        id
                        databaseId
                        url
                        author {{
                          login
                        }}
                        authorAssociation
                        body
                        bodyText
                        createdAt
                        updatedAt
                        publishedAt
                        lastEditedAt
                        isMinimized
                        minimizedReason
                        reactions(first: 10) {{
                          totalCount
                          nodes {{
                            content
                            user {{
                              login
                            }}
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """

            result = run_command([
                "gh", "api", "graphql",
                "-f", f"query={query}"
            ])

            data = json.loads(result.stdout)
            items_data = data.get("data", {}).get("repository", {}).get(type_name)

            if not items_data:
                break

            for item in items_data["nodes"]:
                item_number = str(item["number"])
                comments = []

                for comment in item["comments"]["nodes"]:
                    # Filter by updated_since if provided
                    if updated_since:
                        comment_updated = comment["updatedAt"]
                        if comment_updated <= updated_since:
                            continue

                    comments.append(_parse_comment_node(comment))

                if comments:
                    comments_by_number[item_number] = comments

                # Note: This simplified version only fetches first 100 comments per issue/PR
                # If pagination is needed for comments, that would require individual queries

            has_next_page = items_data["pageInfo"]["hasNextPage"]
            end_cursor = items_data["pageInfo"]["endCursor"]

        return comments_by_number

    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Error getting repository comments for {repository}: {e}", file=sys.stderr)
        return {}


async def monitor_issue_comments(
    js,
    base_path: Path,
    active_issues: list[tuple[str, str]],
    dry_run: bool = False
) -> int:
    """
    Monitor comments on active issues and publish events for new comments.
    Uses a single query per repository to fetch all comments since the last check.

    Args:
        js: JetStream context
        base_path: Base path for issue directories
        active_issues: List of tuples (repository, issue_number)
        dry_run: If True, don't publish events

    Returns:
        Number of new comments discovered
    """
    new_comments_count = 0
    current_time = datetime.now(timezone.utc).isoformat()

    # Group issues by repository
    issues_by_repo: dict[str, list[str]] = {}
    for repository, issue_number in active_issues:
        if repository not in issues_by_repo:
            issues_by_repo[repository] = []
        issues_by_repo[repository].append(issue_number)

    # Process each repository
    for repository, issue_numbers in issues_by_repo.items():
        # Get the earliest last check timestamp for this repository
        last_check = get_repository_last_comment_check(base_path, repository, "issue")

        if last_check:
            print(f"Checking issue comments for {repository} since {last_check}...")
        else:
            print(f"Checking issue comments for {repository} (first check)...")

        # Fetch all comments for the repository with a single query
        all_comments = get_all_repository_comments(repository, "issue", last_check)

        if all_comments:
            total_comments = sum(len(comments) for comments in all_comments.values())
            print(f"  Found {total_comments} new/updated comment(s) across {len(all_comments)} issue(s)")

            # Process comments for each active issue
            for issue_number in issue_numbers:
                comments = all_comments.get(issue_number, [])

                for comment in comments:
                    # Check if this comment is newer than the last check for this specific issue
                    issue_last_check = get_last_comment_check(base_path, repository, issue_number, "issue")
                    if issue_last_check and comment["updated_at"] <= issue_last_check:
                        continue

                    event_data = {
                        "repository": repository,
                        "issue_number": issue_number,
                        **comment
                    }

                    if dry_run:
                        print(f"    [DRY RUN] Would publish github.issue.comment.new event for comment by {comment['author']} on #{issue_number}")
                    else:
                        await publish_event(js, "github.issue.comment.new", event_data)
                        new_comments_count += 1
        else:
            print(f"  No new comments")

        # Update the last check timestamp for each issue
        if not dry_run:
            for issue_number in issue_numbers:
                save_last_comment_check(base_path, repository, issue_number, current_time, "issue")

    return new_comments_count


async def monitor_pr_comments(
    js,
    base_path: Path,
    active_prs: list[tuple[str, str]],
    dry_run: bool = False
) -> int:
    """
    Monitor comments on active pull requests and publish events for new comments.
    Uses a single query per repository to fetch all comments since the last check.

    Args:
        js: JetStream context
        base_path: Base path for PR directories
        active_prs: List of tuples (repository, pr_number)
        dry_run: If True, don't publish events

    Returns:
        Number of new comments discovered
    """
    new_comments_count = 0
    current_time = datetime.now(timezone.utc).isoformat()

    # Group PRs by repository
    prs_by_repo: dict[str, list[str]] = {}
    for repository, pr_number in active_prs:
        if repository not in prs_by_repo:
            prs_by_repo[repository] = []
        prs_by_repo[repository].append(pr_number)

    # Process each repository
    for repository, pr_numbers in prs_by_repo.items():
        # Get the earliest last check timestamp for this repository
        last_check = get_repository_last_comment_check(base_path, repository, "pr")

        if last_check:
            print(f"Checking PR comments for {repository} since {last_check}...")
        else:
            print(f"Checking PR comments for {repository} (first check)...")

        # Fetch all comments for the repository with a single query
        all_comments = get_all_repository_comments(repository, "pr", last_check)

        if all_comments:
            total_comments = sum(len(comments) for comments in all_comments.values())
            print(f"  Found {total_comments} new/updated comment(s) across {len(all_comments)} PR(s)")

            # Process comments for each active PR
            for pr_number in pr_numbers:
                comments = all_comments.get(pr_number, [])

                for comment in comments:
                    # Check if this comment is newer than the last check for this specific PR
                    pr_last_check = get_last_comment_check(base_path, repository, pr_number, "pr")
                    if pr_last_check and comment["updated_at"] <= pr_last_check:
                        continue

                    event_data = {
                        "repository": repository,
                        "number": pr_number,
                        **comment
                    }

                    if dry_run:
                        print(f"    [DRY RUN] Would publish github.pr.comment.new event for comment by {comment['author']} on #{pr_number}")
                    else:
                        await publish_event(js, "github.pr.comment.new", event_data)
                        new_comments_count += 1
        else:
            print(f"  No new comments")

        # Update the last check timestamp for each PR
        if not dry_run:
            for pr_number in pr_numbers:
                save_last_comment_check(base_path, repository, pr_number, current_time, "pr")

    return new_comments_count


async def run_monitoring_cycle(args, nc, js):
    """Run a single monitoring cycle."""
    # Determine which repositories to track
    if args.repositories:
        repositories = args.repositories
    else:
        repositories = get_tracked_repositories(args.path)

    if not repositories:
        print("No repositories to track. Specify repositories with --repositories owner/repo", file=sys.stderr)
        return 1

    print(f"Tracking repositories: {', '.join(repositories)}\n")

    # Monitor repositories and publish events for new issues (if enabled)
    if args.monitor_issues:
        new_count = await monitor_repositories(
            js, args.path, repositories, args.dry_run, args.updated_since, item_type="issue"
        )
        if new_count > 0:
            print(f"Discovered {new_count} new issue(s)\n")

    # Monitor repositories and publish events for new PRs (if enabled)
    if args.monitor_prs:
        new_count = await monitor_repositories(
            js, args.path, repositories, args.dry_run, args.updated_since, item_type="pr"
        )
        if new_count > 0:
            print(f"Discovered {new_count} new PR(s)\n")

    # Find all active issues/PRs (if monitoring or comment monitoring is enabled)
    active_issues = []
    active_prs = []
    need_to_scan = (args.monitor_issues or args.monitor_prs or args.monitor_issue_comments or args.monitor_pr_comments)

    if need_to_scan:
        if args.active_only:
            print(f"Scanning {args.path} for active issues/PRs (with .active flag)...")
        else:
            print(f"Scanning {args.path} for all issues/PRs...")
        all_active_items = find_active_issues(args.path, args.active_only, repositories)

        if not all_active_items:
            print("No active issues/PRs found.")
            # Don't return early - we might still want to monitor comments
        else:
            # Separate issues from PRs
            for repository, number in all_active_items:
                if is_pull_request(repository, number, args.path):
                    active_prs.append((repository, number))
                else:
                    active_issues.append((repository, number))

            print(f"Found {len(active_issues)} active issue(s) and {len(active_prs)} active PR(s)\n")

            # Process active issues (if monitoring enabled)
            if args.monitor_issues and active_issues:
                await process_active_issues(js, args.path, active_issues, args.dry_run, item_type="issue")

            # Process active PRs (if monitoring enabled)
            if args.monitor_prs and active_prs:
                await process_active_issues(js, args.path, active_prs, args.dry_run, item_type="pr")

    # Monitor issue comments if enabled
    if args.monitor_issue_comments and active_issues:
        print("Monitoring issue comments...")
        comment_count = await monitor_issue_comments(js, args.path, active_issues, args.dry_run)
        if comment_count > 0:
            print(f"Found {comment_count} new issue comment{'s' if comment_count != 1 else ''}\n")
        else:
            print()

    # Monitor PR comments if enabled
    if args.monitor_pr_comments and active_prs:
        print("Monitoring PR comments...")
        pr_count = await monitor_pr_comments(js, args.path, active_prs, args.dry_run)
        if pr_count > 0:
            print(f"Found {pr_count} new PR comment{'s' if pr_count != 1 else ''}\n")
        else:
            print()

    return 0


async def main_async(args):
    """Main async function."""
    # Connect to NATS
    nc = NATS()
    js = None

    try:
        if not args.dry_run:
            print(f"Connecting to NATS at {args.nats_server}...")
            await nc.connect(args.nats_server)
            print("Connected to NATS\n")

            # Ensure JetStream stream exists and get JetStream context
            js = await ensure_jetstream_stream(nc)
            print()

        # If interval is specified, run in a loop
        if args.interval:
            print(f"Running monitoring every {args.interval} seconds. Press Ctrl+C to stop.\n")
            cycle_count = 0
            try:
                while True:
                    cycle_count += 1
                    cycle_start = datetime.now(timezone.utc)
                    print(f"=== Monitoring Cycle {cycle_count} at {cycle_start.isoformat()} ===\n")

                    await run_monitoring_cycle(args, nc, js)

                    cycle_end = datetime.now(timezone.utc)
                    elapsed_seconds = (cycle_end - cycle_start).total_seconds()

                    # Calculate remaining time to maintain fixed interval
                    sleep_duration = max(0, args.interval - elapsed_seconds)

                    if sleep_duration > 0:
                        print(f"=== Cycle {cycle_count} completed in {elapsed_seconds:.2f}s. Waiting {sleep_duration:.2f}s until next cycle ===\n")
                        await asyncio.sleep(sleep_duration)
                    else:
                        print(f"=== Cycle {cycle_count} completed in {elapsed_seconds:.2f}s. Cycle took longer than interval ({args.interval}s), starting next cycle immediately ===\n")
            except KeyboardInterrupt:
                print("\n\n=== Monitoring interrupted by user ===")
                print(f"Completed {cycle_count} monitoring cycle(s)")
                print("Shutting down gracefully...\n")
        else:
            # Run once
            await run_monitoring_cycle(args, nc, js)

    except KeyboardInterrupt:
        print("\n\n=== Monitoring interrupted by user ===")
        print("Shutting down gracefully...\n")
    finally:
        if not args.dry_run and nc.is_connected:
            print("Closing NATS connection...")
            await nc.close()
            print("NATS connection closed.")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Monitor GitHub issues and publish events to NATS",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Base path containing repository/issue_number directories"
    )
    parser.add_argument(
        "--repositories",
        nargs="+",
        help="List of repositories to track (format: owner/repo). If not provided, uses existing directories."
    )
    parser.add_argument(
        "--nats-server",
        default="nats://localhost:4222",
        help="NATS server URL"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--updated-since",
        help="Filter issues/PRs updated since this ISO8601 timestamp (e.g., 2024-01-01T00:00:00Z)"
    )
    parser.add_argument(
        "--monitor-issues",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Monitor and publish events for issues (new, updated, closed)"
    )
    parser.add_argument(
        "--monitor-prs",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Monitor and publish events for pull requests (new, updated, closed)"
    )
    parser.add_argument(
        "--monitor-issue-comments",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Monitor and publish events for new comments on active issues"
    )
    parser.add_argument(
        "--monitor-pr-comments",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Monitor and publish events for new comments on active pull requests"
    )
    parser.add_argument(
        "--active-only",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Only monitor issues/PRs with .active flag. Use --no-active-only to monitor all directories."
    )
    parser.add_argument(
        "--interval",
        type=str,
        help="Run monitoring at this interval (format: AdBhCmDs, e.g., 5m, 1h30m, 2d). If not specified, runs once and exits."
    )

    args = parser.parse_args()

    # Parse interval if provided
    if args.interval:
        try:
            args.interval = parse_duration(args.interval)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    # Check dependencies
    if not check_gh_installed():
        print("Error: gh CLI is not installed. Install it from https://cli.github.com/", file=sys.stderr)
        sys.exit(1)

    # Run async main with proper keyboard interrupt handling
    try:
        exit_code = asyncio.run(main_async(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Handle interrupt at top level (though main_async should catch it)
        print("\n\n=== Monitoring interrupted ===", file=sys.stderr)
        sys.exit(130)  # Standard exit code for SIGINT


if __name__ == "__main__":
    main()
