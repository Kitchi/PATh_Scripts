#! /usr/bin/env python

import argparse
import casatasks

parser = argparse.ArgumentParser()
parser.add_argument('--vis', required=True)
parser.add_argument('--channum', required=True)
parser.add_argument('--outvis', required=True)

args = parser.parse_args()

casatasks.split(vis=args.vis, outputvis=args.outvis, spw=f"0:{args.channum}", datacolumn='data')
