#!/usr/bin/env python

import argparse
import math

from casatools import msmetadata

msmd = msmetadata()


def parse_channels(channel_str, nchan):
    if channel_str == 'all':
        return list(range(nchan))
    return [int(c) for c in channel_str.split(',')]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate tclean_jobs.txt for tclean.py')

    parser.add_argument('input_MS', type=str, nargs='+',
                        help='Path(s) to Measurement Set(s)')
    parser.add_argument('--field', type=int, default=1, help='Field index (default: 1)')
    parser.add_argument('--spws', type=str, default='all',
                        help='Comma-separated SPW indices, or "all" (default: all)')
    parser.add_argument('--channels', type=str, default='all',
                        help='Comma-separated channel numbers, or "all" (default: all)')
    parser.add_argument('--channel_chunks', type=int, default=1,
                        help='Channels per tclean job (default: 1)')
    parser.add_argument('--gridder', type=str, default='standard')
    parser.add_argument('--imsize', type=int, default=128)
    parser.add_argument('--cell', type=str, default='0.004arcsec')
    parser.add_argument('--stokes', type=str, default='I')
    parser.add_argument('--niter', type=int, default=0)
    parser.add_argument('--usemask', type=str, default='user')
    parser.add_argument('--threshold', type=str, default='0.0mJy')
    parser.add_argument('--output', type=str, default='tclean_jobs.txt',
                        help='Output filename (default: tclean_jobs.txt)')
    args = parser.parse_args()

    tclean_opts = (
        f"--field {args.field} "
        f"--gridder \"{args.gridder}\" "
        f"--imsize {args.imsize} "
        f"--cell \"{args.cell}\" "
        f"--stokes \"{args.stokes}\" "
        f"--niter {args.niter} "
        f"--usemask \"{args.usemask}\" "
        f"--threshold \"{args.threshold}\""
    )

    lines = []

    # Collect metadata from all MS files
    ms_metadata = {}
    for ms_path in args.input_MS:
        msmd.open(ms_path)
        nf = msmd.nfields()
        print(f"{ms_path}")
        print(f"  nfields: {nf}")
        for fid in range(nf):
            print(f"    field {fid}: name={msmd.namesforfields(fid)}, spws={msmd.spwsforfield(fid)}")
        spws = msmd.spwsforfield(args.field)
        nchan_per_spw = {spw: msmd.nchan(spw) for spw in spws}
        print(f"  -> Using field {args.field}: spws={spws}, nchan_per_spw={nchan_per_spw}")
        msmd.close()
        ms_metadata[ms_path] = {'spws': spws, 'nchan_per_spw': nchan_per_spw}

    # Determine common SPWs across all MS files
    all_spws = [set(md['spws']) for md in ms_metadata.values()]
    common_spws = sorted(set.intersection(*all_spws))

    if args.spws != 'all':
        selected_spws = [int(s) for s in args.spws.split(',')]
        common_spws = [s for s in common_spws if s in selected_spws]

    # For each common SPW, find the max nchan across all MS files
    ms_paths_str = ' '.join(args.input_MS)

    for spw in common_spws:
        nchan = max(md['nchan_per_spw'][spw] for md in ms_metadata.values())
        if args.channels == 'all':
            nblocks = math.ceil(nchan / args.channel_chunks)
            for block_idx in range(nblocks):
                line = (
                    f"python tclean.py {ms_paths_str} "
                    f"--spw {spw} --channel_number {block_idx} "
                    f"--nchannels {args.channel_chunks} "
                    f"{tclean_opts}"
                )
                lines.append(line)
        else:
            channels = parse_channels(args.channels, nchan)
            for chan in channels:
                block_idx = chan // args.channel_chunks
                line = (
                    f"python tclean.py {ms_paths_str} "
                    f"--spw {spw} --channel_number {block_idx} "
                    f"--nchannels {args.channel_chunks} "
                    f"{tclean_opts}"
                )
                lines.append(line)

    with open(args.output, 'w') as f:
        f.write('\n'.join(lines) + '\n')

    print(f"Wrote {len(lines)} jobs to {args.output}")
