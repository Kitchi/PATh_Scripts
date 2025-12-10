#!/usr/bin/env python3
"""
Directory watcher that removes new files/folders.

Polls a directory every 5 minutes and removes any new content.
Terminates after 5 successive polls with no new content.
"""

import argparse
import os
import shutil
import time
from pathlib import Path
from typing import Set


def get_directory_contents(directory: Path) -> Set[Path]:
    """
    Get all files and directories in the given directory (recursively).

    Args:
        directory: Path to directory to scan

    Returns:
        Set of Path objects for all items in the directory
    """
    contents = set()

    if not directory.exists():
        return contents

    # Get all items recursively
    for item in directory.rglob('*'):
        contents.add(item)

    return contents


def remove_items(items: Set[Path]) -> None:
    """
    Remove the given files and directories.

    Args:
        items: Set of Path objects to remove
    """
    # Sort by depth (deepest first) to avoid removing parent before children
    sorted_items = sorted(items, key=lambda p: len(p.parts), reverse=True)

    for item in sorted_items:
        try:
            if item.exists():
                if item.is_file() or item.is_symlink():
                    item.unlink()
                    print(f"Removed file: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"Removed directory: {item}")
        except Exception as e:
            print(f"Error removing {item}: {e}")


def watch_directory(directory: Path, poll_interval: int = 300, empty_polls_threshold: int = 5) -> None:
    """
    Watch directory and remove new content.

    Args:
        directory: Path to directory to watch
        poll_interval: Time in seconds between polls (default: 300 = 5 minutes)
        empty_polls_threshold: Number of consecutive empty polls before terminating (default: 5)
    """
    if not directory.exists():
        raise FileNotFoundError(f"Directory does not exist: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {directory}")

    print(f"Starting directory watcher for: {directory}")
    print(f"Poll interval: {poll_interval} seconds ({poll_interval/60:.1f} minutes)")
    print(f"Will terminate after {empty_polls_threshold} consecutive polls with no new content")
    print("-" * 80)

    # Get initial state
    initial_contents = get_directory_contents(directory)
    print(f"Initial scan: {len(initial_contents)} items found")
    print("-" * 80)

    consecutive_empty_polls = 0
    poll_count = 0

    while consecutive_empty_polls < empty_polls_threshold:
        # Wait for poll interval
        print(f"\nWaiting {poll_interval} seconds until next poll...")
        time.sleep(poll_interval)

        poll_count += 1
        print(f"\n[Poll #{poll_count}] Scanning directory at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Get current contents
        current_contents = get_directory_contents(directory)

        # Find new items (items in current but not in initial)
        new_items = current_contents - initial_contents

        if new_items:
            print(f"Found {len(new_items)} new item(s):")
            for item in sorted(new_items):
                print(f"  - {item}")

            # Remove new items
            print(f"\nRemoving {len(new_items)} new item(s)...")
            remove_items(new_items)

            # Reset consecutive empty polls counter
            consecutive_empty_polls = 0
            print(f"Consecutive empty polls reset to 0")
        else:
            consecutive_empty_polls += 1
            print(f"No new items found")
            print(f"Consecutive empty polls: {consecutive_empty_polls}/{empty_polls_threshold}")

    print("\n" + "=" * 80)
    print(f"Reached {empty_polls_threshold} consecutive polls with no new content")
    print("Terminating watcher")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Watch a directory and remove any new files/folders that appear",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/watch
  %(prog)s /path/to/watch --poll-interval 60
  %(prog)s /path/to/watch --empty-polls 3
        """
    )

    parser.add_argument(
        'directory',
        type=Path,
        help='Directory to watch'
    )

    parser.add_argument(
        '--poll-interval',
        type=int,
        default=300,
        help='Time in seconds between polls (default: 300 = 5 minutes)'
    )

    parser.add_argument(
        '--empty-polls',
        type=int,
        default=5,
        help='Number of consecutive empty polls before terminating (default: 5)'
    )

    args = parser.parse_args()

    try:
        watch_directory(
            directory=args.directory,
            poll_interval=args.poll_interval,
            empty_polls_threshold=args.empty_polls
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user (Ctrl+C)")
        print("Terminating watcher")
    except Exception as e:
        print(f"\nError: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
