#!/usr/bin/env python

import os
import numpy as np
import shutil
import argparse
from casatasks import tclean, exportfits, casalog
logfile = casalog.logfile()


if __name__ == '__main__':

    description = "Image each selected channel, reading from the split out MS. "\
    "Since this script is run independently of the split script, the same split "\
    "parameters need to be passed in to reconstruct the output image file name"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_MS', type=str, nargs='+', help='Measurement Set(s) to image')
    parser.add_argument('--imagename', type=str, default=None,
                        help='Base name for output image (default: common prefix of input MS basenames)')
    parser.add_argument('--channel_number', type=int, help='Channel number to split')
    parser.add_argument('--nchannels', type=int, default=1, help='Number of channels to split')
    parser.add_argument('--field', type=int, default=1, help='Field index')
    parser.add_argument('--spw', type=int, default=0, help='SPW from which to split the channels')
    parser.add_argument('--gridder', type=str, default='standard', help='Gridder to use')
    parser.add_argument('--imsize', type=int, default=128, help='Image size in pixels')
    parser.add_argument('--cell', type=str, default='0.004arcsec', help='Cell size')
    parser.add_argument('--stokes', type=str, default='IQ', help='Stokes parameters to image')
    parser.add_argument('--niter', type=int, default=0, help='Number of iterations')
    parser.add_argument('--usemask', type=str, default='user', help='Masking mode')
    parser.add_argument('--threshold', type=str, default='0.0mJy', help='Threshold for cleaning')
    args = parser.parse_args()

    #casalog.setlogfile('logs/{SLURM_JOB_NAME}_{SLURM_JOB_ID}.casa'.format(**os.environ))

    # Determine imagename base
    if args.imagename:
        imagename_base = args.imagename
    else:
        basenames = [os.path.basename(ms).replace('.ms', '') for ms in args.input_MS]
        imagename_base = os.path.commonprefix(basenames).rstrip('_')

    imagename = f"{imagename_base}_spw_{args.spw:02d}_channel_{args.channel_number:04d}"

    chan_beg = args.channel_number * args.nchannels

    retdict = tclean(vis=args.input_MS, imagename=imagename, imsize=args.imsize,
            cell=args.cell, specmode='cube', selectdata=True,
            field=str(args.field), spw=f"{args.spw}", start=f"{chan_beg}",
            nchan=f"{args.nchannels}", width=1, outframe="LSRK",
            interpolation='nearest', usemask=args.usemask,
            stokes=args.stokes,gridder=args.gridder, niter=args.niter,
            threshold=args.threshold, parallel=False, fullsummary=True)

    np.save(imagename + '_retdict.npy', retdict)

    exportfits(imagename=imagename+'.image', fitsimage=imagename+'.fits', overwrite=True)
    # Zip up the cube to save space - astropy.fits should be able to handle gz FITS for concat
    #os.system(f'gzip {imagename}.fits\n')

    # Clean up the intermediate files
    exts = ['.psf', '.residual', '.sumwt', '.weight', '.pb', '.image', '.model', '.ms', '.mask']
    for ext in exts:
        imname = imagename + ext
        if os.path.isdir(imname):
            shutil.rmtree(imname)
        elif os.path.isfile(imname):
            os.remove(imname)

    #if os.path.exists(imagename+'.fits'):
    #    os.remove(imagename+'.fits')

