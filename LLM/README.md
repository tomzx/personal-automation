# GitHub PR Comments Collector

This script collects all the comments you've left in Pull Requests on GitHub and includes code context for each comment. The output is a markdown file where each comment and its context is separated by `---`.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a GitHub Personal Access Token:
   - Go to GitHub → Settings → Developer Settings → Personal Access Tokens
   - Generate a new token with the following scopes:
     - `repo` (for private repositories)
     - `public_repo` (for public repositories)
     - `read:user` (to get your username)

## Usage

### Basic Usage
```bash
python collect_github_pr_comments.py
```
This will prompt you for your GitHub token and collect all your PR comments.

### With Environment Variable
```bash
export GITHUB_TOKEN=your_token_here
python collect_github_pr_comments.py
```

### Command Line Options
```bash
# Specify token directly
python collect_github_pr_comments.py --token your_token_here

# Specify username (optional)
python collect_github_pr_comments.py --username your_username

# Search specific repositories only
python collect_github_pr_comments.py --repos owner/repo1 owner/repo2

# Limit number of comments
python collect_github_pr_comments.py --limit 50

# Specify output file
python collect_github_pr_comments.py --output my_comments.md
```

## Output Format

The script generates a markdown file with the following structure for each comment:

```markdown
---

## Comment ID: 123456

**Repository:** owner/repo
**Created:** 2023-10-01T12:00:00Z
**URL:** https://github.com/owner/repo/pull/123#issuecomment-123456

**File:** `src/main.py`
**Line:** 42

### Code Context:

```
    39: def some_function():
    40:     # Some code
    41:     value = calculate_something()
>>> 42:     return value
    43: 
    44: def another_function():
    45:     pass
```

### Comment:

This looks good, but we should add error handling here.

---
```

## Features

- Collects all PR comments made by the authenticated user
- Includes code context (3 lines before and after the commented line)
- Shows diff hunks when available
- Handles pagination for large numbers of comments
- Supports filtering by specific repositories
- Rate limit handling
- Comprehensive error handling

## Rate Limits

The script respects GitHub's API rate limits. If you hit the rate limit, it will inform you and exit. You can run it again later.

## Troubleshooting

- **Authentication Error**: Make sure your token has the correct scopes
- **Rate Limit**: Wait for the rate limit to reset (usually 1 hour)
- **No Comments Found**: Check that you've actually made PR comments, or try specifying specific repositories
- **Missing Context**: Some older comments might not have complete context available