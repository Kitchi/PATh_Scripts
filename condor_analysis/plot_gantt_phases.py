#!/usr/bin/env python3
"""
Plot phase breakdown Gantt chart from HTCondor job history parquet file.

This script creates a Gantt chart visualization showing the three phases of job execution:
- Input Transfer (blue)
- Job Execution (green)
- Output Transfer (orange)

Usage:
    python plot_gantt_phases.py <parquet_file> [--output OUTPUT_PDF] [--jobs N] [--job-range START END]

Examples:
    # Plot all jobs to PDF (default)
    python plot_gantt_phases.py condor_jobs.parquet

    # Plot with custom output filename
    python plot_gantt_phases.py condor_jobs.parquet --output my_gantt.pdf

    # Plot first 100 jobs
    python plot_gantt_phases.py condor_jobs.parquet --jobs 100

    # Plot specific job range
    python plot_gantt_phases.py condor_jobs.parquet --job-range 0 200
"""

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from datetime import datetime
import numpy as np


def plot_gantt_phases(df, output_file='gantt_phases.pdf', max_jobs=None, job_range=None):
    """
    Create a Gantt chart showing job phases.

    Args:
        df: pandas.DataFrame with timing columns
        output_file: Path to output image file
        max_jobs: Maximum number of jobs to plot (from start)
        job_range: Tuple of (start_idx, end_idx) for job range to plot
    """
    print(f"Creating phase breakdown Gantt chart...")

    # Filter jobs based on parameters
    if job_range:
        start_idx, end_idx = job_range
        df_plot = df.iloc[start_idx:end_idx].copy()
        print(f"Plotting jobs {start_idx} to {end_idx} ({len(df_plot)} jobs)")
    elif max_jobs:
        df_plot = df.head(max_jobs).copy()
        print(f"Plotting first {len(df_plot)} jobs")
    else:
        df_plot = df.copy()
        print(f"Plotting all {len(df_plot)} jobs")

    # Sort by job start time for better visualization
    df_plot = df_plot.sort_values('job_start_time').reset_index(drop=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, max(8, len(df_plot) * 0.05)))

    # Define phase colors
    colors = {
        'input': '#1f77b4',   # Blue
        'execution': '#2ca02c',  # Green
        'output': '#ff7f0e'   # Orange
    }

    # Track phases for statistics
    phase_stats = {
        'input': [],
        'execution': [],
        'output': []
    }

    # Plot each job's phases
    for idx, row in df_plot.iterrows():
        y_pos = idx

        # Input Transfer phase
        if pd.notna(row['input_start_time']) and pd.notna(row['input_end_time']):
            start = row['input_start_time']
            end = row['input_end_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                ax.barh(y_pos, duration/60, left=mdates.date2num(start)*24*60,
                       height=0.8, color=colors['input'], alpha=0.8)
                phase_stats['input'].append(duration)

        # Job Execution phase
        if pd.notna(row['input_end_time']) and pd.notna(row['output_start_time']):
            start = row['input_end_time']
            end = row['output_start_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                ax.barh(y_pos, duration/60, left=mdates.date2num(start)*24*60,
                       height=0.8, color=colors['execution'], alpha=0.8)
                phase_stats['execution'].append(duration)

        # Output Transfer phase
        if pd.notna(row['output_start_time']) and pd.notna(row['output_end_time']):
            start = row['output_start_time']
            end = row['output_end_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                ax.barh(y_pos, duration/60, left=mdates.date2num(start)*24*60,
                       height=0.8, color=colors['output'], alpha=0.8)
                phase_stats['output'].append(duration)

    # Calculate statistics
    print("\nPhase Duration Statistics (seconds):")
    for phase, durations in phase_stats.items():
        if durations:
            avg = np.mean(durations)
            median = np.median(durations)
            print(f"  {phase.capitalize():10s}: mean={avg:.1f}s, median={median:.1f}s")

    # Format x-axis
    # Convert to datetime for proper formatting
    ax.set_xlim(left=0)  # Will be auto-adjusted

    # Labels and title
    ax.set_xlabel('Time (minutes from start)', fontsize=12)
    ax.set_ylabel('Job Index', fontsize=12)
    ax.set_title('HTCondor Job Phase Breakdown', fontsize=14, fontweight='bold')

    # Y-axis
    ax.set_ylim(-0.5, len(df_plot) - 0.5)
    if len(df_plot) <= 50:
        ax.set_yticks(range(0, len(df_plot), max(1, len(df_plot)//20)))
    else:
        ax.set_yticks(range(0, len(df_plot), max(1, len(df_plot)//10)))

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=colors['input'], alpha=0.8, label='Input Transfer'),
        Patch(facecolor=colors['execution'], alpha=0.8, label='Job Execution'),
        Patch(facecolor=colors['output'], alpha=0.8, label='Output Transfer')
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', axis='x')

    # Tight layout
    plt.tight_layout()

    # Save figure as PDF (vector format, infinitely scalable)
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    print(f"\nPlot saved to: {output_file} (PDF - vector format, infinitely scalable)")

    # Close figure to free memory
    plt.close(fig)


def plot_gantt_phases_datetime(df, output_file='gantt_phases.pdf', max_jobs=None, job_range=None):
    """
    Create a Gantt chart showing job phases with datetime x-axis.

    Args:
        df: pandas.DataFrame with timing columns
        output_file: Path to output image file
        max_jobs: Maximum number of jobs to plot (from start)
        job_range: Tuple of (start_idx, end_idx) for job range to plot
    """
    print(f"Creating phase breakdown Gantt chart...")

    # Filter jobs based on parameters
    if job_range:
        start_idx, end_idx = job_range
        df_plot = df.iloc[start_idx:end_idx].copy()
        print(f"Plotting jobs {start_idx} to {end_idx} ({len(df_plot)} jobs)")
    elif max_jobs:
        df_plot = df.head(max_jobs).copy()
        print(f"Plotting first {len(df_plot)} jobs")
    else:
        df_plot = df.copy()
        print(f"Plotting all {len(df_plot)} jobs")

    # Sort by job start time for better visualization
    df_plot = df_plot.sort_values('job_start_time').reset_index(drop=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, max(8, len(df_plot) * 0.05)))

    # Define phase colors
    colors = {
        'input': '#1f77b4',   # Blue
        'execution': '#2ca02c',  # Green
        'output': '#ff7f0e'   # Orange
    }

    # Track phases for statistics
    phase_stats = {
        'input': [],
        'execution': [],
        'output': []
    }

    # Plot each job's phases
    for idx, row in df_plot.iterrows():
        y_pos = idx

        # Input Transfer phase
        if pd.notna(row['input_start_time']) and pd.notna(row['input_end_time']):
            start = row['input_start_time']
            end = row['input_end_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                width = mdates.date2num(end) - mdates.date2num(start)
                ax.barh(y_pos, width, left=mdates.date2num(start),
                       height=0.8, color=colors['input'], alpha=0.8)
                phase_stats['input'].append(duration)

        # Job Execution phase
        if pd.notna(row['input_end_time']) and pd.notna(row['output_start_time']):
            start = row['input_end_time']
            end = row['output_start_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                width = mdates.date2num(end) - mdates.date2num(start)
                ax.barh(y_pos, width, left=mdates.date2num(start),
                       height=0.8, color=colors['execution'], alpha=0.8)
                phase_stats['execution'].append(duration)

        # Output Transfer phase
        if pd.notna(row['output_start_time']) and pd.notna(row['output_end_time']):
            start = row['output_start_time']
            end = row['output_end_time']
            duration = (end - start).total_seconds()
            if duration > 0:  # Only plot positive durations
                width = mdates.date2num(end) - mdates.date2num(start)
                ax.barh(y_pos, width, left=mdates.date2num(start),
                       height=0.8, color=colors['output'], alpha=0.8)
                phase_stats['output'].append(duration)

    # Calculate statistics
    print("\nPhase Duration Statistics (seconds):")
    for phase, durations in phase_stats.items():
        if durations:
            avg = np.mean(durations)
            median = np.median(durations)
            print(f"  {phase.capitalize():10s}: mean={avg:.1f}s, median={median:.1f}s")

    # Format x-axis for datetime
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Labels and title
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Job Index', fontsize=12)
    ax.set_title('HTCondor Job Phase Breakdown', fontsize=14, fontweight='bold')

    # Y-axis
    ax.set_ylim(-0.5, len(df_plot) - 0.5)
    if len(df_plot) <= 50:
        ax.set_yticks(range(0, len(df_plot), max(1, len(df_plot)//20)))
    else:
        ax.set_yticks(range(0, len(df_plot), max(1, len(df_plot)//10)))

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=colors['input'], alpha=0.8, label='Input Transfer'),
        Patch(facecolor=colors['execution'], alpha=0.8, label='Job Execution'),
        Patch(facecolor=colors['output'], alpha=0.8, label='Output Transfer')
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', axis='x')

    # Tight layout
    plt.tight_layout()

    # Save figure as PDF (vector format, infinitely scalable)
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    print(f"\nPlot saved to: {output_file} (PDF - vector format, infinitely scalable)")

    # Close figure to free memory
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description='Plot phase breakdown Gantt chart from HTCondor job history parquet file'
    )
    parser.add_argument(
        'parquet_file',
        type=str,
        help='Path to HTCondor jobs parquet file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='gantt_phases.pdf',
        help='Output PDF file path (default: gantt_phases.pdf)'
    )
    parser.add_argument(
        '--jobs',
        type=int,
        help='Maximum number of jobs to plot (from start)'
    )
    parser.add_argument(
        '--job-range',
        nargs=2,
        type=int,
        metavar=('START', 'END'),
        help='Plot specific job range (start and end indices)'
    )
    parser.add_argument(
        '--use-datetime',
        action='store_true',
        help='Use datetime x-axis instead of minutes from start'
    )

    args = parser.parse_args()

    # Validate input file
    parquet_path = Path(args.parquet_file)
    if not parquet_path.exists():
        print(f"Error: File not found: {args.parquet_file}")
        return 1

    # Load parquet file
    print(f"Loading {args.parquet_file}...")
    df = pd.read_parquet(args.parquet_file)

    if len(df) == 0:
        print("Error: No data found in parquet file")
        return 1

    # Validate required columns
    required_cols = ['input_start_time', 'input_end_time', 'output_start_time', 'output_end_time']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return 1

    # Create plot
    if args.use_datetime:
        plot_gantt_phases_datetime(df, args.output, args.jobs, tuple(args.job_range) if args.job_range else None)
    else:
        plot_gantt_phases_datetime(df, args.output, args.jobs, tuple(args.job_range) if args.job_range else None)

    return 0


if __name__ == '__main__':
    exit(main())
