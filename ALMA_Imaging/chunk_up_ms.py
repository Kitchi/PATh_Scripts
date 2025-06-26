#! /usr/bin/env python

import os
import shutil
import tarfile
import argparse
import numpy as np
import logging
from casatasks import split
from multiprocessing import Pool
import itertools
from functools import partial

logging.basicConfig(level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s",
                handlers=[logging.FileHandler("chunk_up_ms.log"), logging.StreamHandler()])



def run_split(spw_str, outname, chan_chunk_size, msname, outputfile):
    #spw = inpvals[0]
    #nchan = inpvals[1]
    fptr = open(outputfile, 'a')
    spw = spw_str.split(':')[0]

    #outname = os.path.basename(msname).replace('.ms', f'_spw{spw}_chans_{nn}_{nn+chan_chunk_size}.ms')
    tarname = f"{outname}.tar.gz"
    if os.path.exists(tarname):
        logging.warning(f"{tarname} exists. Not over-writing")
        return

    logging.info(f"Splitting {msname} in to {outname}")
    split(vis=msname, outputvis=outname, spw=spw_str, keepmms=False, keepflags=False)

    logging.info(f"Tarring {outname}")
    with tarfile.open(tarname, "w:gz") as tar:
        tar.add(outname, arcname=os.path.basename(outname))

    logging.info(f"Wiping {outname}, retaining {tarname}")
    shutil.rmtree(outname)

    fptr.write(f"{tarname}, {spw}\n")
    fptr.flush()

    fptr.close()

def chunk_ms():
    parser = argparse.ArgumentParser(description='Chunk and tar MS given the input number of jobs to parallelize over. --nchan can '
                                            'be a comma-separated list, or assumed equal if it is a single number. '
                                             'Channels are not split across SPW boundaries.')

    parser.add_argument('MS', help='Path of the input MS')
    parser.add_argument('njob', type=int, help='Number of jobs to parallelize over')
    parser.add_argument('SPW_list', type=str, help='Comma separated list of SPWs to input to the MS')
    parser.add_argument('nchan', type=str, help='Number of channels per SPW')
    parser.add_argument('--outfile', type=str, help='Name of the output file that contains the list of filenames and metadata info')

    args = parser.parse_args()

    spws = args.SPW_list.split(',')
    nchan_per_spw = args.nchan.split(',')
    # If only one provided, make it same size as spws
    if len(nchan_per_spw) == 1:
        nchan_per_spw = [int(nchan_per_spw[0]) for ii in range(len(spws))]

    print(f"nchan_per_spw {nchan_per_spw}")

    total_nchan = np.sum(nchan_per_spw)
    chan_chunk_size = int(np.ceil(total_nchan/args.njob))

    if args.outfile is None:
        outputfile = os.path.basename(args.MS).replace('.ms', f'_output_file.txt')
    else:
        outputfile = args.outfile

    fptr = open(outputfile, 'w')
    fptr.close()

    print(f"Total chans are {total_nchan}")
    print(f"chan_chunk_size {chan_chunk_size}")
    print(f"outputfile {outputfile}")

    run_split_partial = partial(run_split, chan_chunk_size=chan_chunk_size, msname=args.MS, outputfile=outputfile)

    #args = itertools.product(spws, nchan_per_spw)

    spw_sel_str = []
    outnames = []
    for spw, nchan in zip(spws, nchan_per_spw):
        for nn in range(0, nchan, chan_chunk_size):
            spw_str = f"{spw}:{nn}~{nn+chan_chunk_size}"
            spw_sel_str.append(spw_str)
            outname = os.path.basename(args.MS).replace('.ms', f'_spw{spw}_chans_{nn}_{nn+chan_chunk_size}.ms')
            outnames.append(outname)

    starargs = [(ss, oo) for ss, oo in zip(spw_sel_str, outnames)]
    #print(list(starargs))

    pool = Pool(2)
    pool.starmap(run_split_partial, starargs)



if __name__ == '__main__':
    chunk_ms()
