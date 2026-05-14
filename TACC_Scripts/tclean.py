#!/usr/bin/env python

import os
import math
import numpy as np
import shutil
import argparse
from multiprocessing import Pool
from casatasks import tclean, exportfits, casalog
from casatools import msmetadata

msmd = msmetadata()


def get_msmd(msname):
    msmd.open(msname)
    fieldnames = msmd.fieldnames()
    nscan_per_field = [len(msmd.scansforfield(ff)) for ff in fieldnames]
    target_field = fieldnames[np.argmax(nscan_per_field)]
    target_spws = msmd.spwsforfield(target_field)
    nchan_per_spw = [msmd.nchan(ss) for ss in target_spws]
    msmd.close()
    return target_spws, nchan_per_spw


def image_channel(work_item):
    msname, spw, chan, global_idx, imagename_base, imsize, cell, gridder, stokes, niter, usemask, threshold = work_item

    imagename = f"{imagename_base}_chan_{global_idx:05d}"

    retdict = tclean(
        vis=msname,
        imagename=imagename,
        imsize=imsize,
        cell=cell,
        specmode='mfs',
        selectdata=True,
        spw=f"{spw}:{chan}",
        usemask=usemask,
        stokes=stokes,
        gridder=gridder,
        niter=niter,
        threshold=threshold,
        parallel=False,
        fullsummary=True,
    )

    np.save(imagename + '_retdict.npy', retdict)
    exportfits(imagename=imagename + '.image', fitsimage=imagename + '.fits', overwrite=True)

    for ext in ['.psf', '.residual', '.sumwt', '.weight', '.pb', '.image', '.model', '.mask']:
        impath = imagename + ext
        if os.path.exists(impath):
            shutil.rmtree(impath)

    return global_idx


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_MS', type=str)
    parser.add_argument('--gridder', type=str, default='standard')
    parser.add_argument('--imsize', type=int, default=128)
    parser.add_argument('--cell', type=str, default='0.004arcsec')
    parser.add_argument('--stokes', type=str, default='I')
    parser.add_argument('--niter', type=int, default=0)
    parser.add_argument('--usemask', type=str, default='user')
    parser.add_argument('--threshold', type=str, default='0.0mJy')
    parser.add_argument('--mem_per_chan', type=float, default=8.0, help='GB per tclean process')
    parser.add_argument('--total_ram', type=float, default=128.0, help='Total node RAM in GB')
    parser.add_argument('--total_cores', type=int, default=144, help='Total cores on node')
    args = parser.parse_args()

    job_id = os.environ.get('SLURM_ARRAY_JOB_ID', os.environ.get('SLURM_JOB_ID', 'local'))
    task_id = int(os.environ.get('SLURM_ARRAY_TASK_ID', 0))
    task_count = int(os.environ.get('SLURM_ARRAY_TASK_COUNT', 1))

    os.makedirs('logs', exist_ok=True)
    casalog.setlogfile(f"logs/{os.environ.get('SLURM_JOB_NAME', 'tclean')}_{job_id}_{task_id}.casa")

    n_parallel = int(args.total_ram // args.mem_per_chan)
    omp_threads = max(1, args.total_cores // n_parallel)
    os.environ['OMP_NUM_THREADS'] = str(omp_threads)

    msname = args.input_MS
    target_spws, nchan_per_spw = get_msmd(msname)

    flat_channels = []
    for spw, nchan in zip(target_spws, nchan_per_spw):
        for c in range(nchan):
            flat_channels.append((spw, c, len(flat_channels)))

    total_channels = len(flat_channels)
    total_batches = math.ceil(total_channels / n_parallel)
    imagename_base = os.path.basename(msname).replace('.ms', '')

    my_batches = [b for b in range(total_batches) if b % task_count == task_id]

    print(f"task {task_id}/{task_count}: {total_channels} channels, {n_parallel} parallel, "
          f"{omp_threads} OMP threads, {len(my_batches)}/{total_batches} batches assigned")

    for batch_idx in my_batches:
        start = batch_idx * n_parallel
        end = min(start + n_parallel, total_channels)
        batch = flat_channels[start:end]

        work_items = [
            (msname, spw, chan, gidx, imagename_base,
             args.imsize, args.cell, args.gridder, args.stokes,
             args.niter, args.usemask, args.threshold)
            for spw, chan, gidx in batch
        ]

        with Pool(len(work_items)) as pool:
            pool.map(image_channel, work_items)
