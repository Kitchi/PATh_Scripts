#!/usr/bin/env python3
"""
Plot duration histograms for HTCondor job phases.

This script creates 3 subplots showing histograms of:
- Input Transfer Duration (blue)
- Job Execution Duration (green)
- Output Transfer Duration (orange)

Each histogram includes mean ± std dev in the legend.

Intelligent X-axis Trimming:
- Detects outliers when mean > 5x median
- Automatically trims X-axis to 99th percentile or numpy's 'auto' bin range
- Shows count and percentage of excluded outlier jobs in title
- Improves visualization of the main data distribution

Usage:
    python plot_duration_histograms.py <parquet_file> [--output OUTPUT_PDF]

Example:
    python plot_duration_histograms.py condor_jobs.parquet --output duration_histograms.pdf
"""

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def plot_duration_histograms(df, output_file='duration_histograms.pdf', show_plot=False):
    """
    Create histograms of job phase durations.

    Args:
        df: pandas.DataFrame with duration columns
        output_file: Path to output PDF file
    """
    print(f"Creating duration histograms...")

    # Calculate execution duration from timestamps if job_duration is not available
    df_plot = df.copy()
    if df_plot['job_duration'].isna().all():
        print("Note: job_duration not available, calculating from timestamps...")
        # Calculate as time between input_end_time and output_start_time
        df_plot['execution_duration'] = (
            (df_plot['output_start_time'] - df_plot['input_end_time']).dt.total_seconds()
        )
    else:
        df_plot['execution_duration'] = df_plot['job_duration']

    # Define phase colors (matching gantt_phases.py)
    colors = {
        'input': '#1f77b4',      # Blue
        'execution': '#2ca02c',   # Green
        'output': '#ff7f0e'       # Orange
    }

    # Create figure with 3 subplots (vertical layout)
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))

    # Phase configurations
    phases = [
        {
            'column': 'input_transfer_duration',
            'title': 'Input Transfer Duration',
            'color': colors['input'],
            'ax': axes[0]
        },
        {
            'column': 'execution_duration',
            'title': 'Job Execution Duration',
            'color': colors['execution'],
            'ax': axes[1]
        },
        {
            'column': 'output_transfer_duration',
            'title': 'Output Transfer Duration',
            'color': colors['output'],
            'ax': axes[2]
        }
    ]

    # Plot each histogram
    for phase in phases:
        ax = phase['ax']
        col = phase['column']

        # Get data, filtering out None/NaN values
        data = df_plot[col].dropna()

        # Filter out negative values (data anomalies)
        data_positive = data[data > 0]

        if len(data_positive) == 0:
            print(f"Warning: No valid data for {phase['title']}")
            continue

        # Calculate statistics
        mean_val = data_positive.mean()
        std_val = data_positive.std()
        median_val = data_positive.median()

        # Check for outliers: mean > 5x median suggests heavy outliers
        outlier_detected = mean_val > 5 * median_val
        x_max = None
        trimmed_label = ""
        data_to_plot = data_positive
        plot_mean = mean_val
        plot_std = std_val

        if outlier_detected:
            # Use numpy's auto binning algorithm to determine reasonable range
            # This uses the Freedman-Diaconis rule or Sturges' formula
            _, bin_edges = np.histogram(data_positive, bins='auto')

            # Calculate 99th percentile as upper limit (excludes top 1% outliers)
            p99 = np.percentile(data_positive, 99)

            # Use the smaller of: bin range upper limit or 99th percentile
            x_max = min(bin_edges[-1], p99)

            # Filter data to trimmed range for re-binning
            data_to_plot = data_positive[data_positive <= x_max]

            # Recompute statistics from trimmed data
            plot_mean = data_to_plot.mean()
            plot_std = data_to_plot.std()

            # Count how many points are excluded
            n_excluded = (data_positive > x_max).sum()
            pct_excluded = 100 * n_excluded / len(data_positive)

            trimmed_label = f" (trimmed, {n_excluded} jobs [{pct_excluded:.1f}%] > {x_max:.1f}s)"

            print(f"\n{phase['title']}:")
            print(f"  Overall Mean: {mean_val:.1f}s ± {std_val:.1f}s")
            print(f"  Trimmed Mean: {plot_mean:.1f}s ± {plot_std:.1f}s")
            print(f"  Median: {median_val:.1f}s")
            print(f"  Outliers detected (mean/median ratio: {mean_val/median_val:.1f}x)")
            print(f"  Trimming X-axis to {x_max:.1f}s (excludes {n_excluded} jobs, {pct_excluded:.1f}%)")
            print(f"  Valid samples: {len(data_positive)}/{len(data)}")
        else:
            print(f"\n{phase['title']}:")
            print(f"  Mean: {mean_val:.1f}s ± {std_val:.1f}s")
            print(f"  Median: {median_val:.1f}s")
            print(f"  Valid samples: {len(data_positive)}/{len(data)}")

        # Plot histogram with trimmed data if outliers detected
        n, bins, patches = ax.hist(data_to_plot, bins=50, color=phase['color'],
                                    alpha=0.7, edgecolor='black', linewidth=0.5)

        # Add vertical line for mean (use trimmed mean if outliers detected)
        ax.axvline(plot_mean, color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {plot_mean:.1f}s ± {plot_std:.1f}s')

        # Add vertical line for median
        ax.axvline(median_val, color='darkred', linestyle=':', linewidth=2,
                   label=f'Median: {median_val:.1f}s')

        # Set X-axis limits explicitly if trimmed
        if x_max is not None:
            ax.set_xlim(left=0, right=x_max)

        # Labels and title
        ax.set_xlabel('Duration (seconds)', fontsize=11)
        ax.set_ylabel('Number of Jobs', fontsize=11)
        ax.set_title(phase['title'] + trimmed_label, fontsize=13, fontweight='bold')

        # Legend
        ax.legend(loc='upper right', fontsize=10)

        # Grid
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')

    # Overall title
    fig.suptitle('HTCondor Job Phase Duration Distributions',
                 fontsize=15, fontweight='bold', y=0.995)

    # Tight layout
    plt.tight_layout(rect=[0, 0, 1, 0.99])

    # Save figure as PDF
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    print(f"\nHistograms saved to: {output_file}")

    # Show plot if requested
    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description='Plot duration histograms for HTCondor job phases'
    )
    parser.add_argument(
        'parquet_file',
        type=str,
        help='Path to HTCondor jobs parquet file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='duration_histograms.pdf',
        help='Output PDF file path (default: duration_histograms.pdf)'
    )
    parser.add_argument(
        '--plot',
        action='store_true',
        help='Show interactive plot (default: save only)'
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
    required_cols = ['input_transfer_duration', 'job_duration', 'output_transfer_duration']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return 1

    # Create histograms
    plot_duration_histograms(df, args.output, args.plot)

    return 0


if __name__ == '__main__':
    exit(main())
