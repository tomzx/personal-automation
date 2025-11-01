# /// script
# dependencies = [
#   "pyyaml",
#   "plyer",
# ]
# ///
"""
Task Tracker - Prompts user about their current work and logs to YAML
"""

import argparse
import random
import re
import sys
import time
import yaml
from datetime import datetime
from pathlib import Path
from plyer import notification


def parse_duration(duration_str):
    """
    Parse duration string in format XdYhZmAs to seconds.
    Examples: "1d", "2h30m", "1d12h", "45m", "30s", "1h30m45s"
    """
    if duration_str.isdigit():
        # If it's just a number, treat as seconds
        return int(duration_str)

    pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
    match = re.match(pattern, duration_str.lower())

    if not match or not any(match.groups()):
        raise ValueError(f"Invalid duration format: {duration_str}")

    days = int(match.group(1) or 0)
    hours = int(match.group(2) or 0)
    minutes = int(match.group(3) or 0)
    seconds = int(match.group(4) or 0)

    total_seconds = (days * 86400) + (hours * 3600) + (minutes * 60) + seconds

    if total_seconds == 0:
        raise ValueError(f"Duration cannot be zero: {duration_str}")

    return total_seconds


def format_duration(seconds):
    """
    Format seconds into XdYhZmAs format.
    Examples: 90 -> "1m30s", 3600 -> "1h", 86400 -> "1d"
    """
    if seconds == 0:
        return "0s"

    parts = []

    days = int(seconds // 86400)
    if days > 0:
        parts.append(f"{days}d")
        seconds %= 86400

    hours = int(seconds // 3600)
    if hours > 0:
        parts.append(f"{hours}h")
        seconds %= 3600

    minutes = int(seconds // 60)
    if minutes > 0:
        parts.append(f"{minutes}m")
        seconds %= 60

    if seconds > 0:
        parts.append(f"{int(seconds)}s")

    return "".join(parts)



def prompt_user(last_task=None, last_timestamp=None, notify=False):
    """Prompt user for what they're working on and record the information."""
    # Send notification if enabled
    if notify:
        try:
            notification.notify(
                title='Task Tracker',
                message='What are you working on?',
                app_name='Task Tracker',
                timeout=10
            )
        except Exception as e:
            print(f"Warning: Could not send notification: {e}")

    print("\n" + "="*50)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Time: {current_time}")

    if last_task:
        if last_timestamp:
            print(f"Last task: {last_task} ({last_timestamp})")
        else:
            print(f"Last task: {last_task}")
        prompt_text = "What are you working on? (press Enter to use last task): "
    else:
        prompt_text = "What are you working on? "

    try:
        task = input(prompt_text).strip()
    except EOFError:
        print("\nEOF detected. Exiting...")
        sys.exit(0)

    # If empty and there's a last task, use it
    if not task and last_task:
        task = last_task
        print(f"Using last task: {task}")
    elif not task:
        print("No task entered. Skipping this iteration.")
        return None, last_task

    # Check if it's the same task as before
    if last_task and task.lower() == last_task.lower():
        print("Same task as before. Continuing...")
        return {
            'timestamp': current_time,
            'task': task
        }, last_task

    # If different task, ask if it's important and urgent
    is_important = None
    is_urgent = None
    if task.lower() != (last_task or "").lower():
        try:
            importance = input("Is this work important? (y/N): ").strip().lower()
            if importance in ['y', 'yes']:
                is_important = True
            else:
                # Default to False (no) for empty or 'n'/'no'
                is_important = False

            urgency = input("Is this work urgent? (y/N): ").strip().lower()
            if urgency in ['y', 'yes']:
                is_urgent = True
            else:
                # Default to False (no) for empty or 'n'/'no'
                is_urgent = False
        except EOFError:
            print("\nEOF detected. Exiting...")
            sys.exit(0)

    return {
        'timestamp': current_time,
        'task': task,
        'important': is_important,
        'urgent': is_urgent
    }, task


def append_to_yaml(data, filepath):
    """Append or update task entry in YAML file."""
    filepath = Path(filepath)

    # Create file if it doesn't exist
    if not filepath.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.touch()

    # Read existing documents
    documents = []
    if filepath.stat().st_size > 0:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip():
                    documents = list(yaml.safe_load_all(content))
        except Exception as e:
            print(f"Warning: Could not read existing file: {e}")

    # Find if we have an existing document for this task
    task_found = False
    for doc in documents:
        if doc and doc.get('task') == data['task']:
            # Append timestamp to existing task
            if 'timestamps' not in doc:
                doc['timestamps'] = []
            doc['timestamps'].append(data['timestamp'])
            task_found = True
            break

    # If task not found, create new document
    if not task_found:
        new_doc = {
            'task': data['task'],
            'timestamps': [data['timestamp']]
        }
        if data.get('important') is not None:
            new_doc['important'] = data['important']
        if data.get('urgent') is not None:
            new_doc['urgent'] = data['urgent']
        documents.append(new_doc)

    # Write all documents back to file
    with open(filepath, 'w', encoding='utf-8') as f:
        for doc in documents:
            f.write('---\n')
            f.write(f"task: \"{doc['task']}\"\n")
            if doc.get('important') is not None:
                f.write(f"important: {str(doc['important']).lower()}\n")
            if doc.get('urgent') is not None:
                f.write(f"urgent: {str(doc['urgent']).lower()}\n")
            f.write('timestamps:\n')
            for ts in doc['timestamps']:
                f.write(f"- \"{ts}\"\n")


def load_last_task(filepath):
    """Load the last task and timestamp from the YAML file."""
    filepath = Path(filepath)

    if not filepath.exists():
        return None, None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            if not content.strip():
                return None, None

            # Parse all YAML documents
            documents = list(yaml.safe_load_all(content))

            # Get the last document with a task and its latest timestamp
            for doc in reversed(documents):
                if doc and 'task' in doc:
                    timestamps = doc.get('timestamps', [])
                    last_timestamp = timestamps[-1] if timestamps else None
                    return doc['task'], last_timestamp
    except Exception as e:
        print(f"Warning: Could not load last task from {filepath}: {e}")

    return None, None


def main():
    parser = argparse.ArgumentParser(
        description='Track what you are working on with periodic prompts'
    )
    parser.add_argument(
        '--min-sleep',
        type=str,
        default='30m',
        help='Minimum sleep duration (default: 30m). Format: XdYhZmAs or seconds'
    )
    parser.add_argument(
        '--max-sleep',
        type=str,
        default='1h',
        help='Maximum sleep duration (default: 1h). Format: XdYhZmAs or seconds'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='task-tracking.yaml',
        help='Output YAML file path (default: task-tracking.yaml)'
    )
    parser.add_argument(
        '--notify',
        action='store_true',
        help='Enable system notifications when prompting'
    )

    args = parser.parse_args()

    # Parse duration strings
    try:
        min_sleep = parse_duration(args.min_sleep)
        max_sleep = parse_duration(args.max_sleep)
    except ValueError as e:
        print(f"Error: {e}")
        return

    # Validate sleep range
    if min_sleep <= 0 or max_sleep <= 0:
        print("Error: Sleep durations must be positive")
        return

    if min_sleep > max_sleep:
        print("Error: min-sleep must be less than or equal to max-sleep")
        return

    print(f"Task Tracker Started")
    print(f"Sleep range: {format_duration(min_sleep)}-{format_duration(max_sleep)}")
    print(f"Output file: {args.output}")
    print(f"Press Ctrl+C to stop")

    # Load last task from file if it exists
    last_task, last_timestamp = load_last_task(args.output)
    if last_task:
        if last_timestamp:
            print(f"Loaded last task from file: {last_task} ({last_timestamp})")
        else:
            print(f"Loaded last task from file: {last_task}")

    try:
        while True:
            data, last_task = prompt_user(last_task, last_timestamp, notify=args.notify)

            if data:
                append_to_yaml(data, args.output)
                print(f"Logged to {args.output}")
                last_timestamp = data['timestamp']

            # Calculate sleep duration using uniform distribution
            sleep_duration = random.uniform(min_sleep, max_sleep)
            print(f"\nSleeping for {format_duration(sleep_duration)}...")
            print(f"Next prompt at approximately: {datetime.fromtimestamp(time.time() + sleep_duration).strftime('%Y-%m-%d %H:%M:%S')}")
            print("(Press Ctrl+C to skip sleep and prompt immediately)")

            try:
                time.sleep(sleep_duration)
            except KeyboardInterrupt:
                print("\n\nSleep interrupted. Starting question flow...")
                continue

    except KeyboardInterrupt:
        print("\n\nTask Tracker stopped by user.")
        print(f"All data saved to {args.output}")


if __name__ == '__main__':
    main()
