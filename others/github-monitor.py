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


def find_active_issues(base_path: Path, active_only: bool = True) -> list[tuple[str, str]]:
    """
    Find all active issues and PRs by scanning directories.
    Directory structure is assumed to be {base_path}/{owner/repo}/{issue_or_pr_number}.

    Args:
        base_path: Base path containing repository directories
        active_only: If True, only return issues/PRs with .active files. If False, return all.

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


def get_open_issues(repository: str, updated_since: Optional[str] = None) -> dict[str, dict]:
    """
    Get the list of open issues and pull requests from GitHub using GraphQL API.

    Args:
        repository: Repository in "owner/repo" format
        updated_since: Optional ISO8601 timestamp to filter issues/PRs updated since this time

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

        # Create query builders with the specific parameters
        def issue_query_builder(cursor):
            return _build_issue_query(owner, repo, filter_clause, cursor)

        def pr_query_builder(cursor):
            return _build_pr_query(owner, repo, filter_clause, cursor)

        # Fetch issues
        items = _fetch_paginated_items(
            repository,
            issue_query_builder,
            ["repository", "issues"],
            _parse_issue_node
        )

        # Fetch pull requests
        pr_items = _fetch_paginated_items(
            repository,
            pr_query_builder,
            ["repository", "pullRequests"],
            _parse_pr_node
        )

        # Merge PRs into items
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
    updated_since: Optional[str] = None
) -> int:
    """
    Monitor repositories and publish events for new open issues and PRs.

    Args:
        js: JetStream context
        base_path: Base path for issue directories
        repositories: List of repositories to monitor
        dry_run: If True, don't publish events
        updated_since: Optional ISO8601 timestamp to filter issues/PRs updated since this time

    Returns:
        Number of new issues/PRs discovered
    """
    new_issues_count = 0
    current_time = datetime.now(timezone.utc).isoformat()

    for repository in repositories:
        if updated_since:
            print(f"Fetching open issues for {repository} updated since {updated_since}...")
        else:
            print(f"Fetching open issues for {repository}...")

        open_items = get_open_issues(repository, updated_since)

        if not open_items:
            print(f"  No open issues/PRs found or error fetching")
            continue

        print(f"  Found {len(open_items)} open issue(s)/PR(s)")

        for number, item_data in open_items.items():
            item_dir = base_path / repository / number
            item_type = item_data.get("type", "issue")

            if not item_dir.exists():
                if item_type == "pr":
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
                    print(f"    [DRY RUN] Would save .type file as '{item_type}'")
                else:
                    await publish_event(js, event_subject, event_data)
                    save_last_checked(base_path, repository, number, current_time)
                    save_type_to_file(base_path, repository, number, item_type)
                    new_issues_count += 1

        print()

    return new_issues_count


async def process_active_issues(
    js,
    base_path: Path,
    active_issues: list[tuple[str, str]],
    dry_run: bool = False
) -> None:
    """Process all active issues and PRs and publish appropriate events."""
    current_time = datetime.now(timezone.utc).isoformat()

    # Group issues by repository
    issues_by_repo: dict[str, list[str]] = {}
    for repository, issue_number in active_issues:
        if repository not in issues_by_repo:
            issues_by_repo[repository] = []
        issues_by_repo[repository].append(issue_number)

    # Process each repository
    for repository, issue_numbers in issues_by_repo.items():
        print(f"Checking open issues/PRs for {repository}...")
        open_items = get_open_issues(repository)

        if not open_items and len(issue_numbers) > 0:
            print(f"  Warning: Could not fetch open issues/PRs for {repository}")
            continue

        # Process each issue/PR for this repository
        for number in issue_numbers:
            print(f"  Processing {repository}#{number}...")

            if number in open_items:
                item_data = open_items[number]
                item_type = item_data.get("type", "issue")

                event_data = {
                    "repository": repository,
                    "number": number,
                    **item_data  # Include all item data from GraphQL
                }

                if item_type == "pr":
                    print(f"    PR is still open")
                    event_subject = "github.pr.updated"
                else:
                    print(f"    Issue is still open")
                    event_subject = "github.issue.updated"

                if dry_run:
                    print(f"    [DRY RUN] Would publish {event_subject} event")
                    print(f"    [DRY RUN] Would save .last_checked timestamp")
                else:
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


async def monitor_issue_comments(
    js,
    base_path: Path,
    active_issues: list[tuple[str, str]],
    dry_run: bool = False
) -> int:
    """
    Monitor comments on active issues and publish events for new comments.

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

    for repository, issue_number in active_issues:
        # Get the last time we checked for comments
        last_check = get_last_comment_check(base_path, repository, issue_number, "issue")

        if last_check:
            print(f"Checking comments on {repository}#{issue_number} since {last_check}...")
        else:
            print(f"Checking comments on {repository}#{issue_number} (first check)...")

        # Fetch comments
        comments = get_issue_comments(repository, issue_number, last_check)

        if comments:
            print(f"  Found {len(comments)} new/updated comment(s)")

            for comment in comments:
                event_data = {
                    "repository": repository,
                    "issue_number": issue_number,
                    **comment  # Include all comment data from GraphQL
                }

                if dry_run:
                    print(f"    [DRY RUN] Would publish github.issue.comment.new event for comment by {comment['author']}")
                else:
                    await publish_event(js, "github.issue.comment.new", event_data)
                    new_comments_count += 1
        else:
            print(f"  No new comments")

        # Update the last check timestamp
        if not dry_run:
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

    for repository, pr_number in active_prs:
        # Get the last time we checked for comments
        last_check = get_last_comment_check(base_path, repository, pr_number, "pr")

        if last_check:
            print(f"Checking comments on {repository}#{pr_number} (PR) since {last_check}...")
        else:
            print(f"Checking comments on {repository}#{pr_number} (PR) (first check)...")

        # Fetch comments
        comments = get_pr_comments(repository, pr_number, last_check)

        if comments:
            print(f"  Found {len(comments)} new/updated comment(s)")

            for comment in comments:
                event_data = {
                    "repository": repository,
                    "pr_number": pr_number,
                    **comment  # Include all comment data from GraphQL
                }

                if dry_run:
                    print(f"    [DRY RUN] Would publish github.pr.comment.new event for comment by {comment['author']}")
                else:
                    await publish_event(js, "github.pr.comment.new", event_data)
                    new_comments_count += 1
        else:
            print(f"  No new comments")

        # Update the last check timestamp
        if not dry_run:
            save_last_comment_check(base_path, repository, pr_number, current_time, "pr")

    return new_comments_count


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

        # Determine which repositories to track
        if args.repositories:
            repositories = args.repositories
        else:
            repositories = get_tracked_repositories(args.path)

        if not repositories:
            print("No repositories to track. Specify repositories with --repositories owner/repo", file=sys.stderr)
            return 1

        print(f"Tracking repositories: {', '.join(repositories)}\n")

        # Monitor repositories and publish events for new issues/PRs (if enabled)
        if args.monitor_issues:
            new_count = await monitor_repositories(
                js, args.path, repositories, args.dry_run, args.updated_since
            )
            if new_count > 0:
                print(f"Discovered {new_count} new issue(s)/PR(s)\n")

        # Find all active issues/PRs (if issue monitoring is enabled)
        active_issues = []
        active_prs = []
        if args.monitor_issues:
            if args.active_only:
                print(f"Scanning {args.path} for active issues/PRs (with .active flag)...")
            else:
                print(f"Scanning {args.path} for all issues/PRs...")
            all_active_items = find_active_issues(args.path, args.active_only)

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

                # Process active issues/PRs and publish events
                await process_active_issues(js, args.path, all_active_items, args.dry_run)
        elif args.monitor_issue_comments or args.monitor_pr_comments:
            # If comment monitoring is enabled but issue monitoring is not,
            # still need to find active issues/PRs for comment checking
            if args.active_only:
                print(f"Scanning {args.path} for active issues/PRs (with .active flag, for comment monitoring)...")
            else:
                print(f"Scanning {args.path} for all issues/PRs (for comment monitoring)...")
            all_active_items = find_active_issues(args.path, args.active_only)
            if not all_active_items:
                print("No active issues/PRs found.")
            else:
                # Separate issues from PRs
                for repository, number in all_active_items:
                    if is_pull_request(repository, number, args.path):
                        active_prs.append((repository, number))
                    else:
                        active_issues.append((repository, number))

                print(f"Found {len(active_issues)} active issue(s) and {len(active_prs)} active PR(s)\n")

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

    finally:
        if not args.dry_run and nc.is_connected:
            await nc.close()

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Monitor GitHub issues and publish events to NATS"
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
        help="NATS server URL (default: nats://localhost:4222)"
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
        help="Monitor and publish events for issues and PRs (new, updated, closed)"
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
        help="Only monitor issues/PRs with .active flag (default: True). Use --no-active-only to monitor all directories."
    )

    args = parser.parse_args()

    # Check dependencies
    if not check_gh_installed():
        print("Error: gh CLI is not installed. Install it from https://cli.github.com/", file=sys.stderr)
        sys.exit(1)

    # Run async main
    exit_code = asyncio.run(main_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
