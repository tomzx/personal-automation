#!/usr/bin/env python3
"""
Auto Screenshot CLI Tool

Takes screenshots of each monitor at specified intervals with configurable
file naming using variables for screen number, date, time, and format.
"""

# /// script
# dependencies = [
#   "mss>=9.0.1",
# ]
# ///

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import mss
except ImportError:
    print("Error: mss library not found. Install with: pip install mss")
    sys.exit(1)


def create_filepath(path_template, screen_num, image_format):
    """Create full file path from template with variable substitution."""
    now = datetime.now()

    variables = {
        'screen': str(screen_num),
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H-%M-%S-%f')[:-3],  # Trim to milliseconds
        'format': image_format.lower()
    }

    # Replace variables in template
    filepath = path_template
    for var, value in variables.items():
        filepath = filepath.replace(f'{{{var}}}', value)

    return Path(filepath)


def take_screenshots(path_template, image_format, interval, max_screenshots, selected_monitors=None):
    """Take screenshots of selected monitors at specified interval."""
    with mss.mss() as sct:
        all_monitors = sct.monitors[1:]  # Skip the "All in One" monitor
        num_monitors = len(all_monitors)

        if selected_monitors:
            # Filter monitors by user selection
            monitors = [all_monitors[i-1] for i in selected_monitors if 1 <= i <= num_monitors]
            print(f"Selected monitors: {selected_monitors}")
        else:
            monitors = all_monitors
            print(f"All monitors selected.")

        print(f"Found {num_monitors} monitor(s)")
        print(f"Path template: {path_template}")
        print(f"Interval: {interval} seconds")
        print("Press Ctrl+C to stop\n")

        screenshot_count = 0

        try:
            while max_screenshots == 0 or screenshot_count < max_screenshots:
                for idx, monitor in enumerate(monitors, 1):
                    # idx is the logical screen number in the filtered list
                    filepath = create_filepath(path_template, idx, image_format)

                    # Create directory if it doesn't exist
                    filepath.parent.mkdir(parents=True, exist_ok=True)

                    # Take screenshot
                    screenshot = sct.grab(monitor)
                    mss.tools.to_png(screenshot.rgb, screenshot.size, output=str(filepath))

                    print(f"Screenshot saved: {filepath}")

                screenshot_count += 1

                if max_screenshots == 0 or screenshot_count < max_screenshots:
                    time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\nStopped. Took {screenshot_count} rounds of screenshots.")


def main():
    parser = argparse.ArgumentParser(
        description="Take screenshots of each monitor at specified intervals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Template Variables:
  {screen}  - Screen/monitor number (1, 2, 3, ...)
  {date}    - Current date (YYYY-MM-DD)
  {time}    - Current time (HH-MM-SS-MS)
  {format}  - Image format/extension

Examples:
  python auto_screenshot.py "./screenshots/{date}/{screen}/{date}-{time}.{format}"
  python auto_screenshot.py "./screens/monitor{screen}_{date}.png" -i 30
        """
    )

    parser.add_argument(
        'path',
        help='Path template with variables (e.g., "./screenshots/{date}/{screen}/{date}-{time}.{format}")'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['png', 'jpg', 'jpeg'],
        default='png',
        help='Image format (default: png)'
    )

    parser.add_argument(
        '-i', '--interval',
        type=int,
        default=60,
        help='Interval between screenshots in seconds (default: 60)'
    )

    parser.add_argument(
        '-c', '--count',
        type=int,
        default=0,
        help='Maximum number of screenshot rounds (0 for unlimited, default: 0)'
    )

    parser.add_argument(
        '-m', '--monitors',
        type=int,
        nargs='+',
        default=None,
        help='List of monitor numbers to screenshot (e.g., "-m 1 2"). Default: all monitors.'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.interval <= 0:
        print("Error: Interval must be greater than 0")
        sys.exit(1)

    if args.count < 0:
        print("Error: Count must be 0 or greater")
        sys.exit(1)

    # Ensure path has the format extension
    if '{format}' not in args.path and not args.path.endswith(f'.{args.format}'):
        args.path += f'.{args.format}'

    take_screenshots(
        args.path,
        args.format,
        args.interval,
        args.count,
        args.monitors,
    )


if __name__ == '__main__':
    main()
