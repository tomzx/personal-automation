import datetime
import re
from argparse import ArgumentParser
from pathlib import Path
import sys


def main():
    argument_parser = ArgumentParser()
    argument_parser.add_argument("file")
    argument_parser.add_argument(
        "--minimum-seconds",
        type=float,
        default=0.1,
        help="Duration (in seconds) of the minimum time gap"
    )

    args = argument_parser.parse_args()
    if args.file == "-":
        file = sys.stdin
    else:
        file = Path(args.file).open("r")

    regex = r"\d{4}-\d{2}-\d{2}(T\d{2}(:\d{2}(:\d{2}(\.\d{3,6})?)?)?)?"

    deltas = []
    last_timestamp = None
    zero_duration = datetime.timedelta(seconds=args.minimum_seconds)
    start_timestamp = 0
    end_timestamp = 0
    timestamp = 0
    for line in file.readlines():
        match = re.match(regex, line)
        if not match:
            continue

        timestamp = datetime.datetime.fromisoformat(match[0])

        if last_timestamp:
            delta = timestamp - last_timestamp

            if delta > zero_duration:
                deltas += [[str(last_timestamp), str(delta)]]
        else:
            start_timestamp = timestamp

        last_timestamp = timestamp
    end_timestamp = timestamp

    deltas = sorted(deltas, key=lambda x: x[1], reverse=True)

    if deltas:
        print("Timestamp", "Duration")
        for timestamp, duration in deltas:
            print(timestamp, duration)

    duration = end_timestamp - start_timestamp
    print(f"Total duration: {duration}")

if __name__ == "__main__":
    main()
