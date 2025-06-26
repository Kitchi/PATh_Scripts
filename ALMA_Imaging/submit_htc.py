#! /usr/bin/env python3.6

import htcondor
import classad

# tclean params
gridder             	 = 'mosaic'
imsize              	 = '8192'
cell                	 = '0.004arcsec'
stokes              	 = 'I'
niter               	 = '100000'
usemask             	 = 'auto-multithresh'
threshold           	 = '2mJy'

with open('input_files.txt', 'r') as fptr:
    input_data = fptr.readlines()

itemdata = [{'input_data':inpdat} for inpdat in input_data]

# Interact with the scheduler
schedd = htcondor.Schedd()

        
job_def = htcondor.Submit({"SingularityImage" : "osdf:///path-facility/data/srikrishna.sekhar/containers/casa-6.6.0-modular.sif",
        "+WantOSPool" : "true",
        "executable" : "tclean.py",
        "arguments" : f"$(input_data) --jobid $(Process) --gridder {gridder} --imsize {imsize} --cell {cell} --stokes {stokes} --niter {niter} --usemask {usemask} --threshold {threshold}",
        "transfer_input_files" : "$(input_data)",
        "should_transfer_input_files" : "YES",
        "when_to_transfer_output" : "ON_EXIT_OR_EVICT",
        "request_cpus" : "1",
        "request_memory" : "50G",
        "request_disk" : "100G",
        "max_retries" : "2",
        "log" : "tclean_$(Process).log",
        "output" : "tclean_$(Process).out",
        "error" : "tclean_$(Process).err",
        })

# Submit job
job = schedd.submit(job_def, itemdata = iter(itemdata))

job_id = job.cluster()
with open('job_id.txt', 'w') as fptr:
    fptr.write("%d\n" % job_id)


print(job)
print(job.cluster())



