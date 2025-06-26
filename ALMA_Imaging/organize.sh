#! /bin/bash

echo "Moving .err and .out"
mkdir -p logs_err_out
mv *.err *.out logs_err_out

echo "Moving .log"
mkdir -p logs
mv *.log logs/

echo "Moving FITS images"
mkdir -p fits_images
mv *.fits fits_images

echo "Moving npy dicts"
mkdir -p npy_dicts
mv *.npy npy_dicts

echo "Moving timing files"
mkdir -p tclean_timing
mv *_timing.txt tclean_timing
