"""
Handle GitHub issue and PR events from NATS JetStream.

This script:
1. Consumes GitHub issue and PR events from a JetStream stream with retention limits
2. Uses a durable consumer to track message processing state
3. Handles different event types:
   - github.issue.new: Creates directory structure for new issues
   - github.issue.updated: Invokes claude to process active issues
   - github.issue.closed: Removes .active file to mark issues as inactive
   - github.pr.new: Creates directory structure for new PRs
   - github.pr.updated: Invokes claude to process active PRs
   - github.pr.closed: Removes .active file to mark PRs as inactive
   - github.issue.comment.new: Handles new comments on issues
   - github.pr.comment.new: Handles new comments on PRs

Directory structure: {base_path}/{owner}/{repo}/{issue_or_pr_number}/
"""

# /// script
# dependencies = [
#   "nats-py>=2.1.0,<3",
# ]
# ///

import argparse
import json
import re
import subprocess
import sys
import termios
import tty
from pathlib import Path

try:
    from nats.aio.client import Client as NATS
    from nats.js.api import ConsumerConfig, DeliverPolicy
    import asyncio
except ImportError:
    print("Error: nats-py is not installed. Install it with: pip install nats-py", file=sys.stderr)
    sys.exit(1)


def getch() -> str:
    """Read a single character from stdin without requiring Enter."""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


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


def check_claude_installed() -> bool:
    """Check if claude CLI is installed."""
    try:
        subprocess.run(["claude", "--help"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def create_issue_directory(base_path: Path, repository: str, number: str | int) -> Path:
    """
    Create a directory for an issue.
    The .active file should be created by the user to mark the issue as tracked.
    Repository format is "owner/repo" which maps to directory structure {base_path}/{owner}/{repo}/{issue_number}.

    Returns:
        Path to the created issue directory
    """
    issue_dir = base_path / repository / str(number)
    issue_dir.mkdir(parents=True, exist_ok=True)
    return issue_dir


def remove_active_file(base_path: Path, repository: str, number: str | int) -> bool:
    """
    Remove the .active file from an issue directory.
    Repository format is "owner/repo" which maps to directory {base_path}/{owner}/{repo}/{issue_number}.
    """
    issue_dir = base_path / repository / str(number)
    active_file = issue_dir / ".active"
    try:
        if active_file.exists():
            active_file.unlink()
            return True
        else:
            print(f"Warning: .active file does not exist at {active_file}", file=sys.stderr)
            return False
    except OSError as e:
        print(f"Error removing .active file from {issue_dir}: {e}", file=sys.stderr)
        return False


def find_template(templates_dir: Path, repository: str, event_name: str) -> Path | None:
    """
    Find a template file for the given event, following the hierarchy:
    1. {owner}/{repo}/{event_name}.md
    2. {owner}/.default/{event_name}.md
    3. .default/{event_name}.md

    If an empty {event_name}.md file is found, stops the hierarchy search (used to ignore events).

    Args:
        templates_dir: Base templates directory
        repository: Repository in "owner/repo" format
        event_name: Event name (e.g., "github.pr.comment.new")

    Returns:
        Path to template file if found, None otherwise
    """
    if not templates_dir or not templates_dir.exists():
        return None

    owner, repo = repository.split("/", 1)
    template_filename = f"{event_name}.md"

    # Check in order: owner/repo -> owner/.default -> .default
    search_paths = [
        templates_dir / owner / repo,
        templates_dir / owner / ".default",
        templates_dir / ".default"
    ]

    for search_path in search_paths:
        if not search_path.exists():
            continue

        # Check for template file
        template_file = search_path / template_filename
        if template_file.exists():
            print(f"[TEMPLATE] Found template at {template_file}")
            return template_file

    return None


def invoke_claude(base_path: Path, repository: str, number: str | int, template_path: Path, claude_verbose: bool = False) -> bool:
    """
    Invoke claude with a template.

    Variables injected before template content:
    - REPOSITORY={repository}
    - NUMBER={number}
    - BASE_DIR={base_path}

    If the template file is empty, skips Claude invocation (used to ignore events).

    Args:
        base_path: Base directory path
        repository: Repository in "owner/repo" format
        number: Issue or PR number
        template_path: Path to template file
        claude_verbose: If True, print raw output directly to stdout instead of parsing JSONL
    """
    try:
        # Read template content
        template_content = template_path.read_text(encoding="utf-8")

        # Check if template is empty (used to ignore events)
        if not template_content.strip():
            print(f"[TEMPLATE] Template file is empty, skipping Claude invocation")
            return True

        # Construct prompt with variables and template content
        prompt = f"REPOSITORY={repository} NUMBER={number} BASE_DIR={base_path}\n\n{template_content}"
        cmd = ["claude", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--allowed-tools", "SlashCommand", "-p", prompt]

        print(f"Calling Claude for {repository}#{number} using {template_path}...")
        print("==== claude ====")

        # If claude_verbose is set, just print raw output directly to stdout
        if claude_verbose:
            result = subprocess.run(cmd, check=True)
            print("\n==== claude ====")
            return result.returncode == 0

        # Stream output and parse JSONL
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        # Read and parse each line of JSONL output
        last_message_id = None
        if process.stdout:
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    # Extract and print content from different message types
                    if data.get("type") == "system" and data.get("subtype") == "init":
                        # Print model and permission mode
                        model = data.get("model")
                        if model:
                            print(f"Model: {model}", flush=True)
                        permission_mode = data.get("permissionMode")
                        if permission_mode:
                            print(f"Permission mode: {permission_mode}\n", flush=True)
                        # Print tools at session initialization
                        tools = data.get("tools", [])
                        if tools:
                            print(f"Available tools: {', '.join(tools)}\n", flush=True)
                        # Print slash commands at session initialization
                        slash_commands = data.get("slash_commands", [])
                        if slash_commands:
                            print(f"Available slash commands: {', '.join(slash_commands)}\n", flush=True)
                    elif data.get("type") == "assistant":
                        message = data.get("message", {})
                        message_id = message.get("id")

                        # Add newline when starting a new message
                        if last_message_id is not None and message_id != last_message_id:
                            print()
                        last_message_id = message_id

                        content = message.get("content", [])
                        for item in content:
                            if isinstance(item, dict):
                                if item.get("type") == "text":
                                    print(item.get("text", ""), end="", flush=True)
                                elif item.get("type") == "tool_use":
                                    tool_name = item.get("name", "unknown")
                                    tool_input = item.get("input", {})
                                    print(f"\n[Tool: {tool_name}]", flush=True)
                                    if tool_input:
                                        print(f"Input: {json.dumps(tool_input, indent=2)}", flush=True)
                except json.JSONDecodeError:
                    # If line is not valid JSON, skip it
                    pass

        # Wait for process to complete
        return_code = process.wait()

        print("\n==== claude ====")

        if return_code != 0:
            stderr_output = process.stderr.read() if process.stderr else ""
            print(f"\nError: Claude exited with code {return_code}", file=sys.stderr)
            if stderr_output:
                print(f"stderr: {stderr_output}", file=sys.stderr)
            return False

        return True
    except subprocess.CalledProcessError as e:
        print(f"Error invoking claude for {repository}#{number}: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Unexpected error invoking claude for {repository}#{number}: {e}", file=sys.stderr)
        return False


def should_process_repository(repository: str, repository_pattern: re.Pattern | None) -> bool:
    """Check if events for this repository should be processed."""
    # If no pattern specified, process all repositories
    if repository_pattern is None:
        return True
    # Check if repository matches the pattern
    return repository_pattern.search(repository) is not None


def should_process_user(username: str, skip_user_pattern: re.Pattern | None) -> bool:
    """Check if events from this user should be processed."""
    # If no pattern specified, process all users
    if skip_user_pattern is None:
        return True
    # If pattern matches, don't process (skip)
    return skip_user_pattern.search(username) is None


class EventHandler:
    """Handle GitHub issue and PR events."""

    def __init__(self, base_path: Path, claude_available: bool = True, templates_dir: Path | None = None, claude_verbose: bool = False):
        self.base_path = base_path
        self.claude_available = claude_available
        self.templates_dir = templates_dir
        self.claude_verbose = claude_verbose

    def _invoke_claude_with_template(self, repository: str, number: str | int, event_name: str, log_prefix: str) -> None:
        """
        Helper method to check Claude availability, find template, and invoke Claude.

        Args:
            repository: Repository in "owner/repo" format
            number: Issue or PR number (can be str or int)
            event_name: Event name for template lookup
            log_prefix: Prefix for log messages (e.g., "NEW ISSUE", "UPDATE PR")
        """
        if not self.claude_available:
            print(f"[{log_prefix}] Claude CLI not available, skipping invocation")
            return

        # Find template for this event
        template_path = find_template(self.templates_dir, repository, event_name)
        if not template_path:
            print(f"[{log_prefix}] No template found for {event_name}, skipping")
            return

        if invoke_claude(self.base_path, repository, number, template_path, self.claude_verbose):
            print(f"[{log_prefix}] Successfully invoked claude")
        else:
            print(f"[{log_prefix}] Failed to invoke claude")

    async def handle_new_issue(self, data: dict) -> None:
        """Handle github.issue.new event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[NEW ISSUE] Creating directory for {repository}#{number}")
        issue_dir = create_issue_directory(self.base_path, repository, number)
        print(f"[NEW ISSUE] Created directory: {issue_dir}")

        self._invoke_claude_with_template(repository, number, "github.issue.new", "NEW ISSUE")

    async def handle_updated_issue(self, data: dict) -> None:
        """Handle github.issue.updated event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[UPDATE ISSUE] Processing {repository}#{number}")

        self._invoke_claude_with_template(repository, number, "github.issue.updated", "UPDATE ISSUE")

    async def handle_closed_issue(self, data: dict) -> None:
        """Handle github.issue.closed event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[CLOSE ISSUE] Marking {repository}#{number} as inactive")
        if remove_active_file(self.base_path, repository, number):
            issue_dir = self.base_path / repository / str(number)
            print(f"[CLOSE ISSUE] Removed .active file from {issue_dir}")
        else:
            print(f"[CLOSE ISSUE] Failed to remove .active file")

        self._invoke_claude_with_template(repository, number, "github.issue.closed", "CLOSE ISSUE")

    async def handle_new_pr(self, data: dict) -> None:
        """Handle github.pr.new event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[NEW PR] Creating directory for {repository}#{number}")
        pr_dir = create_issue_directory(self.base_path, repository, number)
        print(f"[NEW PR] Created directory: {pr_dir}")

        self._invoke_claude_with_template(repository, number, "github.pr.new", "NEW PR")

    async def handle_updated_pr(self, data: dict) -> None:
        """Handle github.pr.updated event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[UPDATE PR] Processing {repository}#{number}")

        self._invoke_claude_with_template(repository, number, "github.pr.updated", "UPDATE PR")

    async def handle_closed_pr(self, data: dict) -> None:
        """Handle github.pr.closed event."""
        repository = data["repository"]
        number = data["number"]

        print(f"[CLOSE PR] Marking {repository}#{number} as inactive")
        if remove_active_file(self.base_path, repository, number):
            pr_dir = self.base_path / repository / str(number)
            print(f"[CLOSE PR] Removed .active file from {pr_dir}")
        else:
            print(f"[CLOSE PR] Failed to remove .active file")

        self._invoke_claude_with_template(repository, number, "github.pr.closed", "CLOSE PR")

    async def handle_issue_comment(self, data: dict) -> None:
        """Handle github.issue.comment.new event."""
        repository = data["repository"]
        number = data["number"]
        comment = data["comment"]

        print(f"[ISSUE COMMENT] New comment on {repository}#{number}")
        print(f"[ISSUE COMMENT] Author: {comment['author']}")
        print(f"[ISSUE COMMENT] Created: {comment['created_at']}")
        print(f"[ISSUE COMMENT] URL: {comment['url']}")

        self._invoke_claude_with_template(repository, number, "github.issue.comment.new", "ISSUE COMMENT")

    async def handle_pr_comment(self, data: dict) -> None:
        """Handle github.pr.comment.new event."""
        repository = data["repository"]
        number = data["number"]
        comment = data["comment"]

        print(f"[PR COMMENT] New comment on {repository}#{number}")
        print(f"[PR COMMENT] Author: {comment['author']}")
        print(f"[PR COMMENT] Created: {comment['created_at']}")
        print(f"[PR COMMENT] URL: {comment['url']}")

        self._invoke_claude_with_template(repository, number, "github.pr.comment.new", "PR COMMENT")


async def message_handler(msg, handler: EventHandler, auto_confirm: bool = True, repository_pattern: re.Pattern | None = None, skip_user_pattern: re.Pattern | None = None):
    """Handle incoming NATS JetStream messages."""
    subject = msg.subject
    try:
        data = json.loads(msg.data.decode())
        repository = data["repository"]
        number = data["number"]
        author = data.get("author")

        print(f"\nReceived event on {subject}")
        print(f"{repository}#{number} by {author}")
        print(f"Link: {data.get("url")}")
        title = data.get("title")
        if title:
            print(f"Title: {title}")

        # Check if we should process this repository
        if not should_process_repository(repository, repository_pattern):
            print(f"Skipping {repository}#{number} (repository not in filter)")
            await msg.ack()
            return

        # Check if we should process this user (for non-comment events)
        if not should_process_user(author, skip_user_pattern):
            print(f"Skipping {repository}#{number} from user {author}")
            await msg.ack()
            return

        # Prompt user to continue if auto_confirm is False
        if not auto_confirm:
            while True:
                print("\nPress Enter to process this event, 's' to skip (or Ctrl+C to exit)... ", end='', flush=True)
                response = getch()
                # Handle Ctrl+C (^C)
                if response == '\x03':
                    print()
                    raise KeyboardInterrupt
                elif response.lower() == 's':
                    print('s')
                    print(f"Skipping {repository}#{number}")
                    await msg.ack()
                    return
                elif response == '\r' or response == '\n':
                    print()  # Newline after Enter
                    break  # Valid key, proceed with processing
                else:
                    print()  # Newline for any other character
                    print(f"Unknown key. Please press Enter to process, 's' to skip, or Ctrl+C to exit.")

        if subject == "github.issue.new":
            await handler.handle_new_issue(data)
        elif subject == "github.issue.updated":
            await handler.handle_updated_issue(data)
        elif subject == "github.issue.closed":
            await handler.handle_closed_issue(data)
        elif subject == "github.pr.new":
            await handler.handle_new_pr(data)
        elif subject == "github.pr.updated":
            await handler.handle_updated_pr(data)
        elif subject == "github.pr.closed":
            await handler.handle_closed_pr(data)
        elif subject == "github.issue.comment.new":
            await handler.handle_issue_comment(data)
        elif subject == "github.pr.comment.new":
            await handler.handle_pr_comment(data)
        # Keep backward compatibility with github.issue.process
        elif subject == "github.issue.process":
            await handler.handle_updated_issue(data)
        else:
            print(f"Unknown subject: {subject}")

        # Acknowledge the message after successful processing
        await msg.ack()

    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}", file=sys.stderr)
        await msg.nak()  # Negative acknowledgment - will be redelivered
    except KeyError as e:
        print(f"Missing required field in event data: {e}", file=sys.stderr)
        await msg.term()  # Terminal error - don't redeliver
    except Exception as e:
        print(f"Error handling message: {e}", file=sys.stderr)
        await msg.nak()  # Negative acknowledgment - will be redelivered


async def main_async(args):
    """Main async function."""
    # Check if claude is available
    claude_available = check_claude_installed()
    if not claude_available:
        print("Warning: Claude CLI is not installed. Will skip Claude invocations.", file=sys.stderr)
        print("Install from: https://github.com/anthropics/anthropic-quickstarts", file=sys.stderr)
        print()

    # Compile regex patterns if provided
    repository_pattern = None
    if args.repositories:
        repository_pattern = re.compile(args.repositories)

    skip_user_pattern = None
    if args.skip_users:
        skip_user_pattern = re.compile(args.skip_users)

    # Create event handler
    handler = EventHandler(
        base_path=args.path,
        claude_available=claude_available,
        templates_dir=args.templates_dir,
        claude_verbose=args.claude_verbose
    )

    # Connect to NATS
    nc = NATS()

    try:
        print(f"Connecting to NATS at {args.nats_server}...")
        await nc.connect(args.nats_server)
        print("Connected to NATS")
        print()

        # Get JetStream context
        js = nc.jetstream()

        # Create or get the consumer with proper configuration
        # For new consumers, start from the beginning of the stream (DeliverPolicy.ALL)
        print(f"Setting up consumer '{args.consumer}' on stream '{args.stream}'...")

        consumer_exists = False
        try:
            # Check if consumer already exists
            consumer_info = await js.consumer_info(args.stream, args.consumer)
            consumer_exists = True
            print(f"Consumer '{args.consumer}' already exists (pending: {consumer_info.num_pending})")

            # If recreate flag is set, delete and recreate the consumer
            if args.recreate_consumer:
                print(f"Recreating consumer as requested...")
                await js.delete_consumer(args.stream, args.consumer)
                consumer_exists = False
        except Exception:
            # Consumer doesn't exist
            pass

        if not consumer_exists:
            # Create consumer with DeliverPolicy.ALL
            print(f"Creating new consumer with DeliverPolicy.ALL...")
            from nats.js.api import ConsumerConfig
            consumer_config = ConsumerConfig(
                durable_name=args.consumer,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy="explicit",
            )
            await js.add_consumer(args.stream, consumer_config)
            consumer_info = await js.consumer_info(args.stream, args.consumer)
            print(f"Created new consumer '{args.consumer}' (pending: {consumer_info.num_pending})")

        # Subscribe to JetStream stream with durable consumer
        print(f"Creating pull subscription...")

        psub = await js.pull_subscribe(
            "github.*",  # Subscribe to all GitHub events (issues, PRs, and comments)
            durable=args.consumer,
            stream=args.stream
        )
        print(f"Subscribed to stream '{args.stream}' with durable consumer '{args.consumer}'")
        print()
        print("Listening for events... (Press Ctrl+C to exit)")
        print()

        # Continuously fetch and process messages
        while True:
            try:
                # Fetch messages in batches
                msgs = await psub.fetch(batch=args.batch_size, timeout=args.fetch_timeout)
                for msg in msgs:
                    await message_handler(msg, handler, args.auto_confirm, repository_pattern, skip_user_pattern)
            except TimeoutError:
                # No messages available, continue polling
                continue
            except Exception as e:
                print(f"Error fetching messages: {e}", file=sys.stderr)
                await asyncio.sleep(1)  # Brief pause before retrying

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        if nc.is_connected:
            await nc.close()

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Handle GitHub issue and PR events from NATS JetStream",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Base path containing repository/issue_number directories"
    )
    parser.add_argument(
        "--templates-dir",
        type=Path,
        help="Templates directory containing markdown files for event handlers"
    )
    parser.add_argument(
        "--nats-server",
        default="nats://localhost:4222",
        help="NATS server URL"
    )
    parser.add_argument(
        "--stream",
        default="GITHUB_EVENTS",
        help="JetStream stream name"
    )
    parser.add_argument(
        "--consumer",
        default="github-event-handler",
        help="Durable consumer name"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of messages to fetch per batch"
    )
    parser.add_argument(
        "--fetch-timeout",
        type=float,
        default=5.0,
        help="Timeout in seconds for fetching messages"
    )
    parser.add_argument(
        "--skip-users",
        type=str,
        default=None,
        help="Regex pattern to skip event handling for matching usernames. If not specified, processes events from all users."
    )
    parser.add_argument(
        "--recreate-consumer",
        action="store_true",
        help="Delete and recreate the consumer (useful for reprocessing all messages)"
    )
    parser.add_argument(
        "--claude-verbose",
        action="store_true",
        help="Print raw Claude CLI output directly to stdout instead of parsing JSONL"
    )
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        default=False,
        help="Automatically process events without confirmation. If not set, prompts after each event."
    )
    parser.add_argument(
        "--repositories",
        type=str,
        default=None,
        help="Regex pattern to filter repositories. If not specified, processes all repositories. Repository format is 'owner/repo'."
    )

    args = parser.parse_args()

    # Run async main
    try:
        exit_code = asyncio.run(main_async(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
