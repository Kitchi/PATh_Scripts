#!/usr/bin/env python3
"""
Master script to analyze HTCondor job history and generate all plots.

This script:
1. Parses HTCondor history JSON to parquet (skipped if exists unless --overwrite-parquet)
2. Generates all analysis plots:
   - Job completion curve
   - Phase breakdown Gantt chart
   - Duration histograms
   - Concurrent jobs over time
3. Saves all outputs in a directory named after the job ID

Usage:
    python analyze_jobs.py <json_file> [--output-dir OUTPUT_DIR] [--overwrite-parquet]

Examples:
    # Auto-detect job ID from filename (e.g., condor_history_944143.json -> 944143/)
    python analyze_jobs.py condor_history_944143.json

    # Custom output directory
    python analyze_jobs.py condor_history.json --output-dir my_analysis

    # Force regenerate parquet from JSON
    python analyze_jobs.py condor_history.json --overwrite-parquet
"""

import argparse
import subprocess
import sys
from pathlib import Path
import re


def extract_job_id(json_file):
    """
    Extract job ID from JSON filename.

    Args:
        json_file: Path to JSON file

    Returns:
        Job ID string or None if not found
    """
    # Try to extract job ID from filename like "condor_history_944143.json"
    filename = Path(json_file).stem
    match = re.search(r'(\d+)', filename)
    if match:
        return match.group(1)
    return None


def run_command(cmd, description):
    """
    Run a command and print status.

    Args:
        cmd: Command list to run
        description: Description of what the command does

    Returns:
        True if successful, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}\n")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed!")
        print(f"Error output:\n{e.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Analyze HTCondor job history and generate all plots'
    )
    parser.add_argument(
        'json_file',
        type=str,
        help='Path to HTCondor history JSON file'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory (default: auto-detect from job ID in filename)'
    )
    parser.add_argument(
        '--resolution',
        type=int,
        default=30,
        help='Time resolution for concurrent jobs plot in seconds (default: 30)'
    )
    parser.add_argument(
        '--gantt',
        action='store_true',
        help='Generate Gantt chart (default: disabled)'
    )
    parser.add_argument(
        '--gantt-jobs',
        type=int,
        help='Limit number of jobs in Gantt chart (default: all jobs)'
    )
    parser.add_argument(
        '--overwrite-parquet',
        action='store_true',
        help='Force overwrite of existing parquet file (default: skip if exists)'
    )

    args = parser.parse_args()

    # Validate input file
    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"Error: File not found: {args.json_file}")
        return 1

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        job_id = extract_job_id(args.json_file)
        if job_id:
            output_dir = Path(f"analysis_{job_id}")
            print(f"Auto-detected job ID: {job_id}")
        else:
            output_dir = Path("analysis_output")
            print(f"Could not extract job ID from filename, using default directory")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Get script directory
    script_dir = Path(__file__).parent

    # Define output file paths
    parquet_file = output_dir / "condor_jobs.parquet"
    completion_curve = output_dir / "completion_curve.pdf"
    gantt_phases = output_dir / "gantt_phases.pdf"
    duration_histograms = output_dir / "duration_histograms.pdf"
    concurrent_jobs = output_dir / "concurrent_jobs.pdf"

    # Track success of each step
    all_success = True

    # Step 1: Convert JSON to parquet
    if parquet_file.exists() and not args.overwrite_parquet:
        print(f"\n{'='*60}")
        print("Step 1: Parquet file already exists, skipping conversion")
        print(f"{'='*60}")
        print(f"Using existing: {parquet_file}")
        print(f"(Use --overwrite-parquet to force regeneration)")
    else:
        cmd = [
            'micromamba', 'run', '-n', 'py312', 'python',
            str(script_dir / 'condor_to_parquet.py'),
            str(json_path),
            '--output', str(parquet_file)
        ]
        if not run_command(cmd, "Step 1: Converting JSON to parquet"):
            return 1

    # Step 2: Generate completion curve
    cmd = [
        'micromamba', 'run', '-n', 'py312', 'python',
        str(script_dir / 'plot_completion_curve.py'),
        str(parquet_file),
        '--output', str(completion_curve)
    ]
    if not run_command(cmd, "Step 2: Generating completion curve"):
        all_success = False

    # Step 3: Generate Gantt chart (optional)
    gantt_generated = False
    if args.gantt:
        cmd = [
            'micromamba', 'run', '-n', 'py312', 'python',
            str(script_dir / 'plot_gantt_phases.py'),
            str(parquet_file),
            '--output', str(gantt_phases)
        ]
        if args.gantt_jobs:
            cmd.extend(['--jobs', str(args.gantt_jobs)])
        if not run_command(cmd, "Step 3: Generating phase breakdown Gantt chart"):
            all_success = False
        else:
            gantt_generated = True
    else:
        print(f"\n{'='*60}")
        print("Step 3: Skipping Gantt chart generation (use --gantt to enable)")
        print(f"{'='*60}")

    # Step 4: Generate duration histograms
    cmd = [
        'micromamba', 'run', '-n', 'py312', 'python',
        str(script_dir / 'plot_duration_histograms.py'),
        str(parquet_file),
        '--output', str(duration_histograms)
    ]
    if not run_command(cmd, "Step 4: Generating duration histograms"):
        all_success = False

    # Step 5: Generate concurrent jobs plot
    cmd = [
        'micromamba', 'run', '-n', 'py312', 'python',
        str(script_dir / 'plot_concurrent_jobs.py'),
        str(parquet_file),
        '--output', str(concurrent_jobs),
        '--resolution', str(args.resolution)
    ]
    if not run_command(cmd, "Step 5: Generating concurrent jobs plot"):
        all_success = False

    # Summary
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"\nOutput directory: {output_dir.absolute()}")
    print(f"\nGenerated files:")
    print(f"  1. {parquet_file.name} - Parsed job data")
    print(f"  2. {completion_curve.name} - Job completion curve")
    if gantt_generated:
        print(f"  3. {gantt_phases.name} - Phase breakdown Gantt chart")
    print(f"  {'4' if gantt_generated else '3'}. {duration_histograms.name} - Duration histograms")
    print(f"  {'5' if gantt_generated else '4'}. {concurrent_jobs.name} - Concurrent jobs over time")

    if all_success:
        print(f"\n✓ All analysis steps completed successfully!")
        return 0
    else:
        print(f"\n⚠ Some analysis steps failed. Check output above for details.")
        return 1


if __name__ == '__main__':
    exit(main())
