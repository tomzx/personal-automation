"""
Monitor GitHub issues and manage their active status.

This script:
1. Fetches all open issues from tracked repositories using gh CLI
2. Creates directory structure {path}/{owner}/{repo}/{issue_number}/ for each open issue
3. Scans for .active files to determine which issues are actively tracked
4. For each tracked issue, checks if it's still open on GitHub
5. If open, invokes claude with /prepare-issue command
6. If closed, removes the .active file to mark it as inactive

Directory structure: {path}/{owner}/{repo}/{issue_number}/.active
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


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


def check_claude_installed() -> bool:
    """Check if claude CLI is installed."""
    try:
        subprocess.run(["claude", "--help"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def find_active_issues(base_path: Path) -> list[tuple[str, str]]:
    """
    Find all active issues by scanning for .active files.
    Directory structure is assumed to be {base_path}/{owner/repo}/{issue_number}.

    Returns:
        List of tuples: (repository in owner/repo format, issue_number)
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

                # Check if this issue is active
                active_file = issue_dir / ".active"
                if active_file.exists():
                    active_issues.append((repository, issue_number))

    return active_issues


def get_open_issues(repository: str) -> set[str]:
    """
    Get the list of open issue numbers from GitHub using gh CLI.

    Returns:
        Set of issue numbers (as strings) that are currently open
    """
    try:
        result = run_command([
            "gh", "issue", "list",
            "--repo", repository,
            "--state", "open",
            "--json", "number",
            "--limit", "1000"
        ])

        data = json.loads(result.stdout)
        return {str(issue["number"]) for issue in data}
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        print(f"Error getting open issues for {repository}: {e}", file=sys.stderr)
        return set()


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


def create_issue_directory(base_path: Path, repository: str, issue_number: str) -> Path:
    """
    Create a directory for an issue.
    The .active file should be created by the user to mark the issue as tracked.
    Repository format is "owner/repo" which maps to directory structure {base_path}/{owner}/{repo}/{issue_number}.

    Returns:
        Path to the created issue directory
    """
    issue_dir = base_path / repository / issue_number
    issue_dir.mkdir(parents=True, exist_ok=True)

    return issue_dir


def sync_open_issues(base_path: Path, repositories: list[str]) -> int:
    """
    Fetch open issues from GitHub and create directories for them.
    Repository format should be "owner/repo".

    Returns:
        Number of new issue directories created
    """
    new_issues_count = 0

    for repository in repositories:
        print(f"Fetching open issues for {repository}...")
        open_issues = get_open_issues(repository)

        if not open_issues:
            print(f"  No open issues found or error fetching issues")
            continue

        print(f"  Found {len(open_issues)} open issue(s)")

        for issue_number in open_issues:
            issue_dir = base_path / repository / issue_number

            if not issue_dir.exists():
                print(f"    Creating directory for {repository}#{issue_number}")
                create_issue_directory(base_path, repository, issue_number)
                new_issues_count += 1

        print()

    return new_issues_count


def remove_active_file(base_path: Path, repository: str, issue_number: str) -> bool:
    """
    Remove the .active file from an issue directory.
    Repository format is "owner/repo" which maps to directory {base_path}/{owner}/{repo}/{issue_number}.
    """
    issue_dir = base_path / repository / issue_number
    active_file = issue_dir / ".active"
    try:
        active_file.unlink()
        return True
    except OSError as e:
        print(f"Error removing .active file from {issue_dir}: {e}", file=sys.stderr)
        return False


def invoke_claude(base_path: Path, repository: str, issue_number: str) -> bool:
    """Invoke claude with the /prepare-issue command."""
    try:
        cmd = ["claude", "-p", f"/prepare-issue {repository} {issue_number} BASE_DIR={base_path}"]
        run_command(cmd, capture_output=False)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error invoking claude for {repository}#{issue_number}: {e}", file=sys.stderr)
        return False


def process_issues(base_path: Path, active_issues: list[tuple[str, str]]) -> None:
    """Process all active issues."""
    # Group issues by repository
    issues_by_repo: dict[str, list[str]] = {}
    for repository, issue_number in active_issues:
        if repository not in issues_by_repo:
            issues_by_repo[repository] = []
        issues_by_repo[repository].append(issue_number)

    # Process each repository
    for repository, issue_numbers in issues_by_repo.items():
        print(f"Checking open issues for {repository}...")
        open_issues = get_open_issues(repository)

        if not open_issues and len(issue_numbers) > 0:
            print(f"  Warning: Could not fetch open issues for {repository}")
            continue

        # Process each issue for this repository
        for issue_number in issue_numbers:
            print(f"  Processing {repository}#{issue_number}...")

            if issue_number in open_issues:
                print(f"    Issue is still open, invoking claude")
                invoke_claude(base_path, repository, issue_number)
            else:
                print(f"    Issue is closed, marking as inactive")
                if remove_active_file(base_path, repository, issue_number):
                    issue_dir = base_path / repository / issue_number
                    print(f"    Removed .active file from {issue_dir}")

        print()


def main():
    parser = argparse.ArgumentParser(
        description="Monitor GitHub issues and manage their active status"
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
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )

    args = parser.parse_args()

    # Check dependencies
    if not check_gh_installed():
        print("Error: gh CLI is not installed. Install it from https://cli.github.com/", file=sys.stderr)
        sys.exit(1)

    if not check_claude_installed():
        print("Warning: Claude CLI is not installed. Will skip Claude invocations.", file=sys.stderr)

    # Determine which repositories to track
    if args.repositories:
        repositories = args.repositories
    else:
        repositories = get_tracked_repositories(args.path)

    if not repositories:
        print("No repositories to track. Specify repositories with --repositories owner/repo", file=sys.stderr)
        sys.exit(1)

    print(f"Tracking repositories: {', '.join(repositories)}\n")

    # Sync open issues from GitHub and create directories
    if args.dry_run:
        print("[DRY RUN] Would fetch open issues and create directories\n")
        for repository in repositories:
            open_issues = get_open_issues(repository)
            print(f"[DRY RUN] {repository}: {len(open_issues)} open issue(s)")
        print()
    else:
        new_count = sync_open_issues(args.path, repositories)
        if new_count > 0:
            print(f"Created {new_count} new issue director{'y' if new_count == 1 else 'ies'}\n")

    # Find all active issues
    print(f"Scanning {args.path} for active issues...")
    active_issues = find_active_issues(args.path)

    if not active_issues:
        print("No active issues found.")
        return

    print(f"Found {len(active_issues)} active issue(s)\n")

    # Process issues
    if args.dry_run:
        # Group by repository for dry run display
        issues_by_repo: dict[str, list[str]] = {}
        for repository, issue_number in active_issues:
            if repository not in issues_by_repo:
                issues_by_repo[repository] = []
            issues_by_repo[repository].append(issue_number)

        for repository, issue_numbers in issues_by_repo.items():
            print(f"[DRY RUN] Would check open issues for {repository}")
            open_issues = get_open_issues(repository)
            for issue_number in issue_numbers:
                issue_dir = args.path / repository / issue_number
                if issue_number in open_issues:
                    print(f"[DRY RUN]   {repository}#{issue_number} is open - would invoke claude")
                else:
                    print(f"[DRY RUN]   {repository}#{issue_number} is closed - would remove .active")
            print()
    else:
        process_issues(args.path, active_issues)


if __name__ == "__main__":
    main()
