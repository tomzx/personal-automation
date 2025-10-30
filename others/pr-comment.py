#!/usr/bin/env python3
# /// script
# dependencies = [
#   "PyGithub>=2.1.1",
#   "python-dotenv>=0.19.0",
# ]
# ///
"""
Submit comments on GitHub Pull Requests at specific lines in files.

This script uses the PyGithub library to interact with the GitHub API.
It can create both general PR comments and line-specific review comments.

Run with uv for automatic dependency management:
    uv run pr-comment.py owner/repo 123 --file src/main.py --line 42 --comment "Fix this"
"""

import os
import argparse
from typing import Optional
from github import Github, Auth
from github.GithubException import GithubException
from github.PullRequestComment import PullRequestComment
from github.IssueComment import IssueComment
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# GitHub API configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def get_github_client() -> Github:
    """
    Initialize and return a GitHub client with authentication.

    Returns:
        Github: Authenticated Github client instance
    """
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN environment variable is not set")

    auth = Auth.Token(GITHUB_TOKEN)
    return Github(auth=auth)


def create_pr_review_comment(
    github_client: Github,
    repo_name: str,
    pr_number: int,
    file_path: str,
    line_number: int,
    comment_body: str,
    commit_id: Optional[str] = None
) -> PullRequestComment:
    """
    Create a review comment on a specific line of a file in a PR.

    Args:
        github_client: Authenticated Github client instance
        repo_name: Repository name in format "owner/repo"
        pr_number: Pull request number
        file_path: Path to the file relative to repository root
        line_number: Line number to comment on
        comment_body: The comment text
        commit_id: Optional specific commit SHA. If not provided, uses the latest commit in the PR.

    Returns:
        The created comment object
    """

    try:
        # Get the repository
        repo = github_client.get_repo(repo_name)

        # Get the pull request
        pr = repo.get_pull(pr_number)

        # If no commit_id provided, use the latest commit in the PR
        if not commit_id:
            commits = list(pr.get_commits())
            commit_id = commits[-1].sha
            print(f"Using latest commit: {commit_id}")

        # Create a review comment on the specific line
        # Note: GitHub API requires the comment to be on a line that was changed in the PR
        comment = pr.create_review_comment(
            body=comment_body,
            commit=repo.get_commit(commit_id),
            path=file_path,
            line=line_number
        )

        print(f"✓ Comment created successfully!")
        print(f"  URL: {comment.html_url}")
        return comment

    except GithubException as e:
        print(f"GitHub API Error: {e.status} - {e.data.get('message', 'Unknown error')}")
        if 'errors' in e.data:
            for error in e.data['errors']:
                print(f"  - {error.get('message', error)}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise


def create_pr_comment(
    github_client: Github,
    repo_name: str,
    pr_number: int,
    comment_body: str
) -> IssueComment:
    """
    Create a general comment on a PR (not tied to a specific line).

    Args:
        github_client: Authenticated Github client instance
        repo_name: Repository name in format "owner/repo"
        pr_number: Pull request number
        comment_body: The comment text

    Returns:
        The created comment object
    """
    try:
        # Get the repository
        repo = github_client.get_repo(repo_name)

        # Get the pull request
        pr = repo.get_pull(pr_number)

        # Create a general comment
        comment = pr.create_issue_comment(comment_body)

        print(f"✓ Comment created successfully!")
        print(f"  URL: {comment.html_url}")
        return comment

    except GithubException as e:
        print(f"GitHub API Error: {e.status} - {e.data.get('message', 'Unknown error')}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise


def list_pr_files(github_client: Github, repo_name: str, pr_number: int) -> None:
    """
    List all files changed in a PR.

    Args:
        github_client: Authenticated Github client instance
        repo_name: Repository name in format "owner/repo"
        pr_number: Pull request number
    """
    try:
        repo = github_client.get_repo(repo_name)
        pr = repo.get_pull(pr_number)

        print(f"\nFiles changed in PR #{pr_number}:")
        for file in pr.get_files():
            print(f"  {file.filename}")
            print(f"    Status: {file.status}")
            print(f"    Additions: +{file.additions}, Deletions: -{file.deletions}")
            print(f"    Changes: {file.changes}")
            print()

    except GithubException as e:
        print(f"GitHub API Error: {e.status} - {e.data.get('message', 'Unknown error')}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit comments on GitHub Pull Requests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a line comment on a specific file
  uvx pr-comment.py owner/repo 123 --file src/main.py --line 42 --comment "This needs refactoring"

  # Create a general PR comment
  uvx pr-comment.py owner/repo 123 --comment "Looks good to me!"

  # List files changed in a PR
  uvx pr-comment.py owner/repo 123 --list-files

  # Specify a commit SHA for the line comment
  uvx pr-comment.py owner/repo 123 --file src/main.py --line 42 --comment "Fix this" --commit abc123

Note: Make sure GITHUB_TOKEN is set in your environment or .env file
        """
    )

    parser.add_argument("repo", type=str, help="Repository name in format 'owner/repo'")
    parser.add_argument("pr_number", type=int, help="Pull request number")
    parser.add_argument("--file", type=str, help="File path for line comment")
    parser.add_argument("--line", type=int, help="Line number for line comment")
    parser.add_argument("--comment", type=str, help="Comment body text")
    parser.add_argument("--commit", type=str, help="Commit SHA (optional, uses latest if not provided)")
    parser.add_argument("--list-files", action="store_true", help="List files changed in the PR")

    args = parser.parse_args()

    try:
        # Authenticate once
        github_client = get_github_client()

        if args.list_files:
            list_pr_files(github_client, args.repo, args.pr_number)
        elif args.file and args.line and args.comment:
            # Create a line-specific comment
            create_pr_review_comment(
                github_client,
                args.repo,
                args.pr_number,
                args.file,
                args.line,
                args.comment,
                args.commit
            )
        elif args.comment:
            # Create a general comment
            create_pr_comment(github_client, args.repo, args.pr_number, args.comment)
        else:
            parser.error("Either provide --list-files, or --comment (with optional --file and --line)")

    except Exception as e:
        print(f"\n❌ Failed to complete operation")
        exit(1)


if __name__ == "__main__":
    main()
