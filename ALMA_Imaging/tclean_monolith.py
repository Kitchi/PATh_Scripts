#!/usr/bin/env python

import os
import re
import sys
import numpy as np
import glob
import time
import shutil
import argparse
import datetime
import tarfile

from casatasks import tclean, exportfits, casalog
logfile = casalog.logfile()

from astropy.io import fits

dtn = datetime.datetime.now()

if __name__ == '__main__':

    description = "Image each selected channel, reading from the split out MS. "\
    "Since this script is run independently of the split script, the same split "\
    "parameters need to be passed in to reconstruct the output image file name"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_MS', type=str, help='File to split. Multiple files are provided comma-separated')
    parser.add_argument('--jobid', type=int, help='Job ID of current job')
    parser.add_argument('--spwlist', type=str, help='Comma separated list of SPWs to image')
    parser.add_argument('--nchan', type=int, help='Number of channels per SPW (assumed equal)')

    parser.add_argument('--gridder', type=str, default='standard', help='Gridder to use')
    parser.add_argument('--imsize', type=int, default=128, help='Image size in pixels')
    parser.add_argument('--cell', type=str, default='0.004arcsec', help='Cell size')
    parser.add_argument('--stokes', type=str, default='I', help='Stokes parameters to image')
    parser.add_argument('--niter', type=int, default=0, help='Number of iterations')
    parser.add_argument('--usemask', type=str, default='user', help='Masking mode')
    parser.add_argument('--threshold', type=str, default='0.0mJy', help='Threshold for cleaning')
    args = parser.parse_args()

    #os.system('ls -ltrh')
    #print('---------------------------------------')
    #sys.stdout.flush()
    #os.system('du -hs')
    #print('---------------------------------------')
    #sys.stdout.flush()

    spws = [spw for spw in args.spwlist.split(',')]
    chanlist = np.arange(args.nchan)
    spw_chan_list = []
    for ss in spws:
        for cc in chanlist:
            spw_chan_list.append(f"{ss}:{cc}")

    fptr = open(f'tclean_{args.jobid}_timing.txt', 'w')
    fptr.write(f"#untar_beg untar_end untar_duration tclean_beg tclean_end tclean_duration\n")

    # Drop everything after ?, such as ?direct or ?auto etc.
    basefile = os.path.basename(args.input_MS.split('?')[0])
    if not os.path.exists(basefile):
        print(f"File {basefile} does not exist. Skipping.")
        exit

    #print("------------------------------------------")
    #print(f"Processing {infile}")
    #os.system("ls -ltrh")
    #os.system("du -hs .")

    untar_beg = time.time()
    print(f"Begin untar {untar_beg}")
    # untargz the file
    with tarfile.open(os.path.basename(basefile), 'r') as tar:
        tar.extractall()
    untar_end = time.time()
    print(f"End untar {untar_end}")

    print(f"Extracting {infile} took {untar_end - untar_beg:.2f}s")

    input_MS = os.path.basename(basefile).strip('.tar')
    imagename = os.path.basename(input_MS).replace('.ms', '.im')

    tclean_beg = time.time()
    print(f"Begin tclean {tclean_beg}")

    retdict = tclean(vis=input_MS, imagename=imagename, imsize=args.imsize,
                        selectdata = True, spw=f"{spw_chan_list[jobid]}",
                        cell=args.cell, specmode='mfs', selectdata=True,
                        usemask=args.usemask, stokes=args.stokes,gridder=args.gridder,
                        niter=args.niter, threshold=args.threshold, parallel=False, fullsummary=True)

    tclean_end = time.time()
    print(f"End tclean {tclean_end}")
    print(f"Imaging {input_MS} took {tclean_end-tclean_beg:.2f}s")

    np.save(imagename + '_retdict.npy', retdict)
    fitsimage = imagename + '.fits'
    exportfits(imagename=imagename+'.image', fitsimage=fitsimage)
    print("Finished exporting FITS file")

    fptr.write(f"{untar_beg} {untar_end} {untar_end - untar_beg} {tclean_beg} {tclean_end} {tclean_end - tclean_beg}\n")

    # Clean up the intermediate files
    exts = ['.psf', '.image', '.residual', '.sumwt', '.weight', '.pb', '.model', '.mask']
    for ext in exts:
        imname = imagename + ext
        if os.path.exists(imname):
            shutil.rmtree(imname)

    if os.path.exists(input_MS + '.tar.gz'):
        os.remove(input_MS + '.tar.gz')

    if os.path.exists(imagename + '.fits'):
        os.remove(imagename + '.fits')

    #if os.path.exists(imagename + '_retdict.npy'):
    #    os.remove(imagename + '_retdict.npy')

    if os.path.exists(input_MS):
        shutil.rmtree(input_MS)

fptr.close()

