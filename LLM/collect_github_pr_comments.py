#!/usr/bin/env python3
"""
GitHub PR Comments Collector

This script collects all PR comments made by the authenticated user
and includes code context for each comment.
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import List, Dict, Optional
import argparse


class GitHubPRCommentsCollector:
    def __init__(self, token: str, username: Optional[str] = None):
        """
        Initialize the collector with GitHub token and optional username.
        
        Args:
            token: GitHub personal access token
            username: GitHub username (if not provided, will be fetched from API)
        """
        self.token = token
        self.username = username
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        self.base_url = 'https://api.github.com'
        
        # Get username if not provided
        if not self.username:
            self.username = self._get_authenticated_user()
    
    def _get_authenticated_user(self) -> str:
        """Get the authenticated user's username."""
        response = requests.get(f'{self.base_url}/user', headers=self.headers)
        if response.status_code == 200:
            return response.json()['login']
        else:
            raise Exception(f"Failed to get authenticated user: {response.status_code}")
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """Make a GET request to the GitHub API."""
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 403 and 'rate limit' in response.text.lower():
            print("Rate limit exceeded. Please wait and try again later.")
            sys.exit(1)
        return response
    
    def _get_all_pages(self, url: str, params: Optional[Dict] = None) -> List[Dict]:
        """Get all pages of results from a paginated API endpoint."""
        all_items = []
        page = 1
        
        while True:
            current_params = params.copy() if params else {}
            current_params['page'] = page
            current_params['per_page'] = 100
            
            response = self._make_request(url, current_params)
            
            if response.status_code != 200:
                print(f"Error fetching page {page}: {response.status_code}")
                break
                
            items = response.json()
            if not items:
                break
                
            all_items.extend(items)
            page += 1
            
            # Check if there are more pages
            if 'Link' not in response.headers:
                break
                
            links = response.headers['Link']
            if 'rel="next"' not in links:
                break
        
        return all_items
    
    def _get_file_content(self, repo_full_name: str, file_path: str, ref: str) -> Optional[str]:
        """Get the content of a file from the repository."""
        url = f'{self.base_url}/repos/{repo_full_name}/contents/{file_path}'
        params = {'ref': ref}
        
        response = self._make_request(url, params)
        
        if response.status_code == 200:
            content_data = response.json()
            if content_data.get('encoding') == 'base64':
                import base64
                return base64.b64decode(content_data['content']).decode('utf-8', errors='ignore')
        
        return None
    
    def _get_code_context(self, comment: Dict, repo_full_name: str) -> Dict:
        """Get code context for a PR comment."""
        context = {
            'file_path': comment.get('path', ''),
            'line_number': comment.get('line', comment.get('original_line', 0)),
            'code_lines': [],
            'diff_hunk': comment.get('diff_hunk', '')
        }
        
        # If we have a diff hunk, use it for context
        if context['diff_hunk']:
            context['code_lines'] = context['diff_hunk'].split('\n')
            return context
        
        # Otherwise, try to get the file content
        if context['file_path'] and context['line_number']:
            file_content = self._get_file_content(
                repo_full_name, 
                context['file_path'], 
                comment.get('commit_id', 'HEAD')
            )
            
            if file_content:
                lines = file_content.split('\n')
                line_num = context['line_number']
                
                # Get 3 lines before and after the commented line
                start_line = max(0, line_num - 4)
                end_line = min(len(lines), line_num + 3)
                
                context['code_lines'] = []
                for i in range(start_line, end_line):
                    prefix = '>>> ' if i == line_num - 1 else '    '
                    context['code_lines'].append(f'{prefix}{i+1}: {lines[i]}')
        
        return context
    
    def collect_pr_comments(self, repos: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict]:
        """
        Collect PR comments made by the authenticated user.
        
        Args:
            repos: List of repositories to search (format: "owner/repo")
            limit: Maximum number of comments to collect
            
        Returns:
            List of comment dictionaries with context
        """
        print(f"Collecting PR comments for user: {self.username}")
        
        all_comments = []
        
        if repos:
            # Search specific repositories
            for repo in repos:
                print(f"Searching repository: {repo}")
                comments = self._get_repo_pr_comments(repo)
                all_comments.extend(comments)
        else:
            # Search all repositories the user has access to
            print("Searching all accessible repositories...")
            all_comments = self._get_all_user_pr_comments()
        
        # Filter comments by the authenticated user
        user_comments = [c for c in all_comments if c.get('user', {}).get('login') == self.username]
        
        if limit:
            user_comments = user_comments[:limit]
        
        print(f"Found {len(user_comments)} comments")
        
        # Add code context to each comment
        comments_with_context = []
        for i, comment in enumerate(user_comments):
            print(f"Processing comment {i+1}/{len(user_comments)}")
            
            # Extract repository name from the comment URL
            repo_full_name = self._extract_repo_from_url(comment['html_url'])
            
            # Get code context
            context = self._get_code_context(comment, repo_full_name)
            
            comment_data = {
                'id': comment['id'],
                'body': comment['body'],
                'created_at': comment['created_at'],
                'updated_at': comment['updated_at'],
                'html_url': comment['html_url'],
                'repository': repo_full_name,
                'context': context
            }
            
            comments_with_context.append(comment_data)
        
        return comments_with_context
    
    def _extract_repo_from_url(self, html_url: str) -> str:
        """Extract repository name from GitHub URL."""
        # URL format: https://github.com/owner/repo/pull/123#issuecomment-456
        parts = html_url.split('/')
        if len(parts) >= 5:
            return f"{parts[3]}/{parts[4]}"
        return ""
    
    def _get_repo_pr_comments(self, repo: str) -> List[Dict]:
        """Get PR comments for a specific repository."""
        url = f'{self.base_url}/repos/{repo}/pulls/comments'
        return self._get_all_pages(url)
    
    def _get_all_user_pr_comments(self) -> List[Dict]:
        """Get all PR comments across all repositories using search API."""
        # Use the search API to find PR comments by the user
        url = f'{self.base_url}/search/issues'
        params = {
            'q': f'commenter:{self.username} type:pr',
            'sort': 'updated',
            'order': 'desc'
        }
        
        search_results = self._get_all_pages(url, params)
        
        # Now get the actual comments for each PR
        all_comments = []
        for pr in search_results:
            if 'pull_request' in pr:
                # Get comments for this PR
                comments_url = pr['pull_request']['url'].replace('/pulls/', '/pulls/').replace('https://api.github.com/repos/', f'{self.base_url}/repos/') + '/comments'
                comments = self._get_all_pages(comments_url)
                all_comments.extend(comments)
        
        return all_comments
    
    def save_to_markdown(self, comments: List[Dict], output_file: str = 'github_pr_comments.md'):
        """Save comments to a markdown file."""
        print(f"Saving {len(comments)} comments to {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# GitHub PR Comments Collection\n\n")
            f.write(f"Collected on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total comments: {len(comments)}\n\n")
            
            for comment in comments:
                f.write("---\n\n")
                
                # Comment metadata
                f.write(f"## Comment ID: {comment['id']}\n\n")
                f.write(f"**Repository:** {comment['repository']}\n")
                f.write(f"**Created:** {comment['created_at']}\n")
                f.write(f"**URL:** {comment['html_url']}\n\n")
                
                # Code context
                if comment['context']['file_path']:
                    f.write(f"**File:** `{comment['context']['file_path']}`\n")
                    if comment['context']['line_number']:
                        f.write(f"**Line:** {comment['context']['line_number']}\n")
                    f.write("\n")
                
                # Code context
                if comment['context']['code_lines']:
                    f.write("### Code Context:\n\n")
                    f.write("```\n")
                    for line in comment['context']['code_lines']:
                        f.write(f"{line}\n")
                    f.write("```\n\n")
                elif comment['context']['diff_hunk']:
                    f.write("### Diff Context:\n\n")
                    f.write("```diff\n")
                    f.write(comment['context']['diff_hunk'])
                    f.write("\n```\n\n")
                
                # Comment body
                f.write("### Comment:\n\n")
                f.write(f"{comment['body']}\n\n")
        
        print(f"Comments saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Collect GitHub PR comments with code context')
    parser.add_argument('--token', help='GitHub personal access token')
    parser.add_argument('--username', help='GitHub username (optional)')
    parser.add_argument('--repos', nargs='*', help='Specific repositories to search (format: owner/repo)')
    parser.add_argument('--limit', type=int, help='Maximum number of comments to collect')
    parser.add_argument('--output', default='github_pr_comments.md', help='Output markdown file')
    
    args = parser.parse_args()
    
    # Get token from argument, environment variable, or prompt
    token = args.token or os.getenv('GITHUB_TOKEN')
    if not token:
        token = input("Enter your GitHub personal access token: ").strip()
    
    if not token:
        print("Error: GitHub token is required")
        sys.exit(1)
    
    try:
        collector = GitHubPRCommentsCollector(token, args.username)
        comments = collector.collect_pr_comments(args.repos, args.limit)
        collector.save_to_markdown(comments, args.output)
        
        print(f"\nCompleted! Found {len(comments)} comments.")
        print(f"Results saved to: {args.output}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()