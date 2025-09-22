#!/usr/bin/env python3
"""
Camera Capture CLI - Capture images from camera using ffmpeg at specified intervals
"""

import argparse
import subprocess
import time
import os
from datetime import datetime
from pathlib import Path
import sys


def get_current_datetime():
    """Get current date and time for path templating"""
    now = datetime.now()
    return {
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H-%M-%S'),
        'year': now.strftime('%Y'),
        'month': now.strftime('%m'),
        'day': now.strftime('%d'),
        'hour': now.strftime('%H'),
        'minute': now.strftime('%M'),
        'second': now.strftime('%S')
    }


def format_path(path_template, image_format):
    """Format the path template with current date/time and format"""
    variables = get_current_datetime()
    variables['format'] = image_format

    try:
        formatted_path = path_template.format(**variables)
        return formatted_path
    except KeyError as e:
        print(f"Error: Unknown variable in path template: {e}")
        sys.exit(1)


def capture_image(output_path, image_format, device=None):
    """Capture an image using ffmpeg"""
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Build ffmpeg command
    cmd = ['ffmpeg', '-y']  # -y to overwrite existing files


    cmd.extend(['-video_size', '1920x1080'])  # Set resolution (adjust as needed)

    # Input device (camera)
    if device:
        cmd.extend(['-f', 'dshow', '-i', f'video={device}'])
    else:
        # Default camera input (adjust based on platform)
        if os.name == 'nt':  # Windows
            cmd.extend(['-f', 'dshow', '-i', 'video="USB Video Device"'])
        elif os.name == 'posix':  # Linux/macOS
            cmd.extend(['-f', 'v4l2', '-i', '/dev/video0'])

    # Capture single frame
    cmd.extend(['-frames:v', '1'])

    # Output format
    if image_format.lower() == 'jpeg':
        cmd.extend(['-q:v', '2'])  # High quality JPEG

    # Output file
    cmd.append(output_path)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error capturing image: {e}")
        print(f"ffmpeg stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: ffmpeg not found. Please install ffmpeg and ensure it's in your PATH.")
        sys.exit(1)


def list_camera_devices():
    """List available camera devices using ffmpeg"""
    print("Listing available camera devices...")

    try:
        if os.name == 'nt':  # Windows
            # List DirectShow video devices
            cmd = ['ffmpeg', '-f', 'dshow', '-list_devices', 'true', '-i', 'dummy']
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

            # Parse the output to extract video devices
            lines = result.stdout.split('\n')
            video_devices = []
            capture_next = False

            for line in lines:
                if '[dshow @' in line and '"' in line:
                    # Extract device name from the line
                    start = line.find('"') + 1
                    end = line.rfind('"')
                    if start > 0 and end > start:
                        device_name = line[start:end]
                        video_devices.append(device_name)

            if video_devices:
                print("\nAvailable video devices:")
                for i, device in enumerate(video_devices, 1):
                    print(f"  {i}. {device}")
                print(f"\nTo use a specific device, use: -d \"{video_devices[0]}\"")
            else:
                print("\nNo video devices found.")

        else:  # Linux/macOS
            # List Video4Linux2 devices
            print("\nScanning for video devices...")
            devices_found = []

            # Check for common video device paths
            for i in range(10):  # Check /dev/video0 through /dev/video9
                device_path = f"/dev/video{i}"
                if os.path.exists(device_path):
                    # Try to get device info using ffmpeg
                    cmd = ['ffmpeg', '-f', 'v4l2', '-list_formats', 'all', '-i', device_path]
                    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

                    if 'Input/output error' not in result.stdout and 'No such file or directory' not in result.stdout:
                        devices_found.append(device_path)

            if devices_found:
                print("Available video devices:")
                for device in devices_found:
                    print(f"  {device}")
                print(f"\nTo use a specific device, use: -d '{devices_found[0]}'")
            else:
                print("No video devices found.")

    except FileNotFoundError:
        print("Error: ffmpeg not found. Please install ffmpeg and ensure it's in your PATH.")
        return False
    except Exception as e:
        print(f"Error listing devices: {e}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Capture images from camera using ffmpeg at specified intervals"
    )

    parser.add_argument(
        'path',
        help='Output path template with variables like {date}, {time}, {format}. '
             'Example: H:\\Camera\\{date}\\{date}-{time}.{format}'
    )

    parser.add_argument(
        '-i', '--interval',
        type=int,
        default=60,
        help='Capture interval in seconds (default: 60)'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['jpeg', 'png'],
        default='jpeg',
        help='Image format (default: jpeg)'
    )

    parser.add_argument(
        '-d', '--device',
        help='Camera device (optional, uses system default if not specified)'
    )

    parser.add_argument(
        '-c', '--count',
        type=int,
        help='Number of images to capture (default: infinite)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be captured without actually capturing'
    )

    parser.add_argument(
        '--list-devices',
        action='store_true',
        help='List available camera devices and exit'
    )

    args = parser.parse_args()

    if args.list_devices:
        list_camera_devices()
        return

    if args.dry_run:
        print("Dry run mode - showing what would be captured:")
        sample_path = format_path(args.path, args.format)
        print(f"Sample output path: {sample_path}")
        print(f"Interval: {args.interval} seconds")
        print(f"Format: {args.format}")
        print(f"Device: {args.device or 'system default'}")
        return

    print(f"Starting camera capture...")
    print(f"Interval: {args.interval} seconds")
    print(f"Format: {args.format}")
    print(f"Device: {args.device or 'system default'}")
    print("Press Ctrl+C to stop")

    captured = 0

    try:
        while True:
            # Format the output path with current date/time
            output_path = format_path(args.path, args.format)

            if capture_image(output_path, args.format, args.device):
                captured += 1
                print(f"Image saved: {output_path}")
            else:
                print("Failed to capture image")

            # Check if we've reached the desired count
            if args.count and captured >= args.count:
                print(f"Captured {captured} images. Exiting.")
                break

            # Wait for the specified interval
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\nStopped. Captured {captured} images total.")


if __name__ == '__main__':
    main()
