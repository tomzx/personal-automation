"""
Background runner is a utility script to run other scripts in the background.
"""

# /// script
# dependencies = [
#     "PyYAML"
# ]
# ///

import os
import subprocess
from pathlib import Path
import argparse
import yaml
import threading
import sys

def read_output(process, prefix):
	"""Read output from process and print with prefix."""
	for line in iter(process.stdout.readline, ''):
		if line:
			print(f"[{prefix}] {line.rstrip()}", flush=True)
	process.stdout.close()

def read_error(process, prefix):
	"""Read error output from process and print with prefix."""
	for line in iter(process.stderr.readline, ''):
		if line:
			print(f"[{prefix}] {line.rstrip()}", file=sys.stderr, flush=True)
	process.stderr.close()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Run scripts in the background from a YAML file")
	parser.add_argument("config_file", type=Path, help="Path to YAML file containing list of scripts")
	args = parser.parse_args()

	with open(args.config_file, 'r') as f:
		scripts = yaml.safe_load(f)

	processes = []
	threads = []
	for script in scripts:
		script_name = script.get('name')
		script_command = script.get('command')

		# Set environment to disable buffering
		env = os.environ.copy()
		env['PYTHONUNBUFFERED'] = '1'

		process = subprocess.Popen(
			script_command,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			shell=True,
			text=True,
			env=env
		)
		processes.append(process)
		print(f"Started [{script_name}] with PID {process.pid}")

		# Start threads to read stdout and stderr
		stdout_thread = threading.Thread(target=read_output, args=(process, script_name), daemon=True)
		stderr_thread = threading.Thread(target=read_error, args=(process, script_name), daemon=True)
		stdout_thread.start()
		stderr_thread.start()
		threads.append(stdout_thread)
		threads.append(stderr_thread)

	# Keep the script running while subprocesses are active
	try:
		for process in processes:
			process.wait()
		# Wait for all output threads to finish
		for thread in threads:
			thread.join(timeout=1)
	except KeyboardInterrupt:
		print("\nTerminating subprocesses...")
		for process in processes:
			process.terminate()
