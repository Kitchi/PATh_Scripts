#!/usr/bin/env python3
"""
Generate interactive Gantt charts from HTCondor job history JSON files.

This script visualizes HTCondor job execution timelines with three phases:
- Input Transfer (blue)
- Job Execution (teal/bluish-green)
- Output Transfer (yellow/gold)
- Failed jobs (vermillion/red-orange)

Colors use the colorblind-friendly Okabe-Ito palette.

Usage:
    python gantt_chart.py <json_file> [--output OUTPUT_HTML]

Example:
    python gantt_chart.py condor_history_944143.json --output gantt_chart.html
"""

import json
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import holoviews as hv
from holoviews import opts

# Enable Bokeh backend
hv.extension('bokeh')


def parse_condor_history(json_file):
    """
    Parse HTCondor history JSON file and extract job timing data.

    Args:
        json_file: Path to HTCondor history JSON file

    Returns:
        pandas.DataFrame with columns: job_id, phase, start_time, end_time, duration, failed
    """
    print(f"Parsing {json_file}...")

    with open(json_file, 'r') as f:
        data = json.load(f)

    records = []

    for job in data:
        proc_id = job.get('ProcId')
        cluster_id = job.get('ClusterId')
        job_status = job.get('JobStatus', 0)
        exit_code = job.get('ExitCode', 0)

        # Determine if job failed (JobStatus != 4 or ExitCode != 0)
        failed = (job_status != 4) or (exit_code != 0)

        # Extract timestamps
        input_start = job.get('JobCurrentStartTransferInputDate')
        input_end = job.get('JobCurrentFinishTransferInputDate')
        output_start = job.get('JobCurrentStartTransferOutputDate')
        output_end = job.get('JobCurrentFinishTransferOutputDate')
        completion_date = job.get('CompletionDate')

        # Input Transfer phase
        if input_start and input_end:
            records.append({
                'job_id': proc_id,
                'cluster_id': cluster_id,
                'phase': 'Input Transfer',
                'start_time': datetime.fromtimestamp(input_start),
                'end_time': datetime.fromtimestamp(input_end),
                'duration': input_end - input_start,
                'failed': failed
            })

        # Job Execution phase
        if input_end and output_start:
            records.append({
                'job_id': proc_id,
                'cluster_id': cluster_id,
                'phase': 'Job Execution',
                'start_time': datetime.fromtimestamp(input_end),
                'end_time': datetime.fromtimestamp(output_start),
                'duration': output_start - input_end,
                'failed': failed
            })

        # Output Transfer phase
        if output_start and output_end:
            records.append({
                'job_id': proc_id,
                'cluster_id': cluster_id,
                'phase': 'Output Transfer',
                'start_time': datetime.fromtimestamp(output_start),
                'end_time': datetime.fromtimestamp(output_end),
                'duration': output_end - output_start,
                'failed': failed
            })
        elif failed and output_start and completion_date:
            # For failed jobs, use completion date as end time
            records.append({
                'job_id': proc_id,
                'cluster_id': cluster_id,
                'phase': 'Output Transfer',
                'start_time': datetime.fromtimestamp(output_start),
                'end_time': datetime.fromtimestamp(completion_date),
                'duration': completion_date - output_start,
                'failed': failed
            })

    df = pd.DataFrame(records)

    print(f"Parsed {len(df)} phase records from {len(data)} jobs")

    return df


def print_statistics(df):
    """Print summary statistics about the job history."""
    total_jobs = df['job_id'].nunique()
    failed_jobs = df[df['failed']]['job_id'].nunique()

    print("\n" + "=" * 60)
    print("Job Statistics")
    print("=" * 60)
    print(f"Total jobs: {total_jobs}")
    print(f"Failed jobs: {failed_jobs}")
    print(f"Success rate: {(total_jobs - failed_jobs) / total_jobs * 100:.1f}%")
    print()

    # Average phase durations
    print("Average Phase Durations:")
    for phase in ['Input Transfer', 'Job Execution', 'Output Transfer']:
        phase_df = df[df['phase'] == phase]
        if len(phase_df) > 0:
            avg_duration = phase_df['duration'].mean()
            print(f"  {phase}: {avg_duration:.1f} seconds ({avg_duration/60:.1f} minutes)")

    print("=" * 60 + "\n")


def create_gantt_chart(df, output_file='gantt_chart.html'):
    """
    Create interactive Gantt chart from job timing data.

    Args:
        df: pandas.DataFrame with job timing data
        output_file: Path to output HTML file
    """
    print(f"Creating Gantt chart...")

    # Define color mapping using colorblind-friendly Okabe-Ito palette
    color_map = {
        'Input Transfer': '#0173B2',   # Blue
        'Job Execution': '#029E73',     # Teal/Bluish-green
        'Output Transfer': '#ECB01F',   # Yellow/Gold
    }

    # Create list of rectangle objects grouped by phase
    overlays = []

    for phase in ['Input Transfer', 'Job Execution', 'Output Transfer']:
        phase_df = df[df['phase'] == phase].copy()

        if len(phase_df) == 0:
            continue

        # Prepare rectangle data: (x0, y0, x1, y1)
        rectangles = []
        for _, row in phase_df.iterrows():
            color = '#DE4A3E' if row['failed'] else color_map[phase]  # Vermillion for failed
            rectangles.append({
                'x0': row['start_time'],
                'y0': row['job_id'] - 0.45,
                'x1': row['end_time'],
                'y1': row['job_id'] + 0.45,
                'job_id': row['job_id'],
                'cluster_id': row['cluster_id'],
                'phase': row['phase'],
                'duration': row['duration'],
                'failed': 'Yes' if row['failed'] else 'No',
                'color': color
            })

        rect_df = pd.DataFrame(rectangles)

        # Create rectangles with proper vdims for hover tooltips
        rects = hv.Rectangles(
            rect_df,
            kdims=['x0', 'y0', 'x1', 'y1'],
            vdims=['job_id', 'cluster_id', 'phase', 'duration', 'failed', 'color'],
            label=phase
        )

        # Apply styling
        rects = rects.opts(
            color='color',
            line_color='color',
            alpha=0.8,
            tools=['hover'],
            hover_tooltips=[
                ('Job ID', '@job_id'),
                ('Cluster', '@cluster_id'),
                ('Phase', '@phase'),
                ('Duration', '@duration{0.1f} sec'),
                ('Failed', '@failed')
            ]
        )

        overlays.append(rects)

    # Combine all phases into overlay
    chart = hv.Overlay(overlays)

    # Configure chart appearance
    chart = chart.opts(
        opts.Overlay(
            width=1000,
            height=700,
            xlabel='Time',
            ylabel='Job ID (ProcId)',
            title='HTCondor Job Execution Timeline',
            legend_position='right',
            show_legend=True,
            toolbar='above',
            active_tools=['pan', 'wheel_zoom'],
            xformatter='%Y-%m-%d %H:%M'
        )
    )

    # Save to HTML
    hv.save(chart, output_file, backend='bokeh')
    print(f"Gantt chart saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate interactive Gantt charts from HTCondor job history JSON files'
    )
    parser.add_argument(
        'json_file',
        type=str,
        help='Path to HTCondor history JSON file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='gantt_chart.html',
        help='Output HTML file path (default: gantt_chart.html)'
    )

    args = parser.parse_args()

    # Validate input file
    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"Error: File not found: {args.json_file}")
        return 1

    # Parse job history
    df = parse_condor_history(args.json_file)

    if len(df) == 0:
        print("Error: No job data found in JSON file")
        return 1

    # Print statistics
    print_statistics(df)

    # Create Gantt chart
    create_gantt_chart(df, args.output)

    print(f"\nDone! Open {args.output} in a web browser to view the chart.")

    return 0


if __name__ == '__main__':
    exit(main())
