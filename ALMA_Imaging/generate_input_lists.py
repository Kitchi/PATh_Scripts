#! /usr/bin/env python

import argparse
import glob
import tarfile
import os

def generate_input_files():
    """
    Given the parallel breadth and data path, generate the input files for the
    HTCondor job submission.

    If there are 1000 files and breadth is 10, each line of the input file will contain 100 files.
    If there are 1000 files and breadth is 1, each line of the input file will contain 1 file.
    """

    parser = argparse.ArgumentParser(description='Generate input files for HTCondor job submission.')
    parser.add_argument('infiles', nargs='*', help='List of files to process')
    parser.add_argument('breadth', type=int, help='Number of concurrent jobs to submit')
    parser.add_argument('-o', '--outfile', help='Output file to write the input files to.', default='input_files.txt')
    parser.add_argument('-d', '--outdir', help='Output directory to write the outfile and optional tarball.', default='input_files.txt')
    parser.add_argument('-t', '--tar', action='store_true', help='Tar up all the input files into a single tarball (originals are preserved)')
    parser.add_argument('-f', '--force', action='store_true', help='Force over-write of tarballs if they already exist')

    args = parser.parse_args()

    infiles = args.infiles
    breadth = args.breadth
    output_file = args.outfile
    output_dir = args.outdir
    do_tar = args.tar
    do_force = args.force

    # Get all the files in the data directory
    infiles = sorted(infiles)
    num_files = len(infiles)
    stride = int(round(num_files/breadth))
    nstep = len(range(0, num_files, stride))

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    output_file = os.path.join(output_dir, os.path.basename(output_file))

    with open(output_file, 'w') as f:
        total_size = 0
        for idx, ii in enumerate(range(0, num_files, stride)):
            input_files = infiles[ii:ii+stride]
            input_files = [os.path.abspath(x) for x in input_files]
            total_size = sum([os.path.getsize(x) for x in input_files])
            input_files = [x.replace('/home/srikrishna.sekhar/data/data/', 'osdf:///path-facility/data/srikrishna.sekhar/data/') for x in input_files]
            f.write(','.join(input_files) + '\n')

            if do_tar:
                tar_file = os.path.join(os.path.basename(output_dir), f'tar_chunk_{idx:04d}.tar')

                if os.path.exists(tar_file) and not do_force:
                    print(f"tarball {tar_file} exists. Skipping.")
                    continue

                print(f"Creating tarball {tar_file}")
                with tarfile.open(tar_file, 'w') as tar:
                    [tar.add(input_file) for input_file in input_files]

            if idx == 0 or idx == num_files - 1:
                print(f"For chunk{idx} total size is {total_size/1e9} GB; Total files are {len(input_files)}.")
                print(f"Memory footprint to request is {2.5 * total_size/1e9} GB (fudge factor of 2.5)")


if __name__ == '__main__':
    generate_input_files()
