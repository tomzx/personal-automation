"""
Handle GitHub issue events from NATS JetStream.

This script:
1. Consumes GitHub issue events from a JetStream stream with retention limits
2. Uses a durable consumer to track message processing state
3. Handles different event types:
   - github.issue.new: Creates directory structure for new issues
   - github.issue.process: Invokes claude to process active issues
   - github.issue.closed: Removes .active file to mark issues as inactive

Directory structure: {base_path}/{owner}/{repo}/{issue_number}/
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
from pathlib import Path

try:
    from nats.aio.client import Client as NATS
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


def check_claude_installed() -> bool:
    """Check if claude CLI is installed."""
    try:
        subprocess.run(["claude", "--help"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


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


def remove_active_file(base_path: Path, repository: str, issue_number: str) -> bool:
    """
    Remove the .active file from an issue directory.
    Repository format is "owner/repo" which maps to directory {base_path}/{owner}/{repo}/{issue_number}.
    """
    issue_dir = base_path / repository / issue_number
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


def invoke_claude(base_path: Path, repository: str, issue_number: str) -> bool:
    """Invoke claude with the /prepare-issue command."""
    try:
        cmd = ["claude", "-p", f"/prepare-issue {repository} {issue_number} BASE_DIR={base_path}"]
        run_command(cmd, capture_output=False)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error invoking claude for {repository}#{issue_number}: {e}", file=sys.stderr)
        return False


class EventHandler:
    """Handle GitHub issue events."""

    def __init__(self, claude_available: bool = True):
        self.claude_available = claude_available

    async def handle_new_issue(self, data: dict) -> None:
        """Handle github.issue.new event."""
        repository = data["repository"]
        issue_number = data["issue_number"]
        base_path = Path(data["base_path"])

        print(f"[NEW] Creating directory for {repository}#{issue_number}")
        issue_dir = create_issue_directory(base_path, repository, issue_number)
        print(f"[NEW] Created directory: {issue_dir}")

    async def handle_process_issue(self, data: dict) -> None:
        """Handle github.issue.process event."""
        repository = data["repository"]
        issue_number = data["issue_number"]
        base_path = Path(data["base_path"])

        print(f"[PROCESS] Processing {repository}#{issue_number}")

        if not self.claude_available:
            print(f"[PROCESS] Claude CLI not available, skipping invocation")
            return

        if invoke_claude(base_path, repository, issue_number):
            print(f"[PROCESS] Successfully invoked claude")
        else:
            print(f"[PROCESS] Failed to invoke claude")

    async def handle_closed_issue(self, data: dict) -> None:
        """Handle github.issue.closed event."""
        repository = data["repository"]
        issue_number = data["issue_number"]
        base_path = Path(data["base_path"])

        print(f"[CLOSED] Marking {repository}#{issue_number} as inactive")
        if remove_active_file(base_path, repository, issue_number):
            issue_dir = base_path / repository / issue_number
            print(f"[CLOSED] Removed .active file from {issue_dir}")
        else:
            print(f"[CLOSED] Failed to remove .active file")


async def message_handler(msg, handler: EventHandler):
    """Handle incoming NATS JetStream messages."""
    subject = msg.subject
    try:
        data = json.loads(msg.data.decode())
        print(f"\nReceived event on {subject}")

        if subject == "github.issue.new":
            await handler.handle_new_issue(data)
        elif subject == "github.issue.process":
            await handler.handle_process_issue(data)
        elif subject == "github.issue.closed":
            await handler.handle_closed_issue(data)
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

    # Create event handler
    handler = EventHandler(claude_available=claude_available)

    # Connect to NATS
    nc = NATS()

    try:
        print(f"Connecting to NATS at {args.nats_server}...")
        await nc.connect(args.nats_server)
        print("Connected to NATS")
        print()

        # Get JetStream context
        js = nc.jetstream()

        # Subscribe to JetStream stream with durable consumer
        # This creates or uses an existing durable consumer
        print(f"Creating pull subscription to stream '{args.stream}' with consumer '{args.consumer}'...")
        psub = await js.pull_subscribe(
            "github.issue.*",  # Subscribe to all GitHub issue events
            durable=args.consumer,
            stream=args.stream,
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
                    await message_handler(msg, handler)
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
        description="Handle GitHub issue events from NATS JetStream"
    )
    parser.add_argument(
        "--nats-server",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)"
    )
    parser.add_argument(
        "--stream",
        default="GITHUB_EVENTS",
        help="JetStream stream name (default: GITHUB_EVENTS)"
    )
    parser.add_argument(
        "--consumer",
        default="github-event-handler",
        help="Durable consumer name (default: github-event-handler)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of messages to fetch per batch (default: 10)"
    )
    parser.add_argument(
        "--fetch-timeout",
        type=float,
        default=5.0,
        help="Timeout in seconds for fetching messages (default: 5.0)"
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
