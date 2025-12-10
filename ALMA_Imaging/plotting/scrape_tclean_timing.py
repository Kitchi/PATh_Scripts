#! /usr/bin/env python

import os
import glob
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class TimingLogs:
    """
    Parse and store tclean timing logs
    """

    def __init__(self, logdir):
        self.logdir = logdir
        self.logs = []
        self.df = pd.DataFrame()

    def parse_logs(self, save_df=False):
        """
        Parse all tclean timing logs in the specified directory.
        Returns a dataframe.
        """
        self.logs = sorted(glob.glob(os.path.join(self.logdir, 'tclean_*_timing.txt')))
        job_ids = [os.path.basename(log).split('_')[1] for log in self.logs]

        dflist = []
        for log_file, job_id in zip(self.logs, job_ids):
            with open(log_file, 'r') as f:
                lines = f.readlines()
                if len(lines) == 0:
                    print(f"Warning: {log_file} is empty.")
                    continue
                keys = lines[0].strip().split()
                values = lines[1].strip().split()
                dflist.append(dict(zip(keys, values)))

        self.df = pd.DataFrame(dflist)
        print(self.df.head())
        if save_df:
            self.df.to_csv(os.path.join(self.logdir, 'tclean_timing_summary.csv'), index=False)
            print(f"DataFrame saved to {os.path.join(self.logdir, 'tclean_timing_summary.csv')}")


def plot_tclean_histogram(df):
    """
    Plot histogram of tclean runtimes
    """

    fix, ax = plt.subplots(figsize=(10, 6))
    hist, bins = np.histogram(df['tclean_duration'].astype(float).values, bins=100, range=(0, 1000))
    median = np.median(df['tclean_duration'].astype(float).values)
    bins = 0.5 * (bins[1:] + bins[:-1])
    w = bins[1] - bins[0]
    print(bins.shape, hist.shape)
    ax.bar(bins, hist, width=w, edgecolor='black', alpha=0.7)
    ax.axvline(median, color='red', linestyle='--', label=f'Median: {median:.2f}s')

    ax.set_xlabel('tclean Duration (s)')
    ax.set_ylabel('Frequency')
    ax.set_title('Histogram of tclean Runtimes')
    ax.legend()

    plt.tight_layout()
    plt.savefig('tclean_runtime_histogram.png', bbox_inches='tight')

def plot_tclean_gantt(df):

    beg = df['tclean_beg'].astype(float).values
    end = df['tclean_end'].astype(float).values

    # Convert from seconds to hours
    beg /= 3600.
    end /= 3600.

    duration = end - beg
    beg -= beg[0]
    job_ids = df.index

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(job_ids, width=duration, left=beg, color='skyblue', edgecolor='black')

    ax.set_xlabel('Time (h)')
    ax.set_ylabel('Job ID')
    ax.set_title('Timeline of tclean Jobs')

    plt.tight_layout()
    plt.savefig('tclean_gantt_chart.png', bbox_inches='tight')

def main():
    """
    Parse the tclean timing logs, turn it into a DataFrame, and perform more
    analysis and plotting.
    """

    parser = argparse.ArgumentParser(description='Process tclean timing logs.')
    parser.add_argument('log_dir', type=str, help='Directory containing tclean timing logs')
    parser.add_argument('--save-df', action='store_true', help='Save the DataFrame to a CSV file')
    args = parser.parse_args()

    lp = TimingLogs(args.log_dir)
    lp.parse_logs(save_df=args.save_df)
    print(f"Plotting tclean histogram")
    plot_tclean_histogram(lp.df)
    print(f"Plotting tclean gantt chart")
    plot_tclean_gantt(lp.df)



if __name__ == '__main__':
    main()
