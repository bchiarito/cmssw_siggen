#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse

#griduser_id = (subprocess.check_output("voms-proxy-info --identity", shell=True).decode('utf-8')).split('/')[5][3:]
griduser_id = (subprocess.check_output("whoami").decode('utf-8')).strip()

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for output eos area')
parser.add_argument('input_jobdir', help='job directory for lhe step')
parser.add_argument('-s', '--split', type=int, default=1, help='split each lhe into this many subjobs')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
args = parser.parse_args()

job_name = args.run_name+'_STEP_splitlhe'
job_dir = 'Job_'+job_name
splitting = args.split
output_eos = '/store/user/'+griduser_id+'/siggen/splitlhe/'+args.run_name
submit_jdl_filename = 'submit_STEP_splitlhe.jdl'
if args.input_jobdir[-1] == '/': args.input_jobdir = args.input_jobdir[:-1]

# find lhe step output area
loc = "."
dirs = []
for d in os.listdir(loc):
  if os.path.isdir(d) and d.startswith(args.input_jobdir):
    dirs.append(os.path.join(loc, d))
if not len(dirs)==1: raise SystemExit("Job Directory pattern returned zero or >1 directories.")
jobDir = dirs[0]
sys.path.append(jobDir)
import job_info as job
input_lhe_location = job.output

# make job directory
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout/')

with open(submit_jdl_filename, 'w') as f:
  f.write(
"""universe = vanilla
initialdir = {}
error  = stdout/$(Cluster)_$(Process)_out.txt
output = stdout/$(Cluster)_$(Process)_out.txt
log    = log_$(Cluster).txt
executable = execute_STEP_splitlhe.sh
transfer_input_files = ../split_helper.py, ../splitLHE.py
arguments = {} {} {} {}
Notification = never
request_memory = 4000
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {}
JobBatchName = {}

queue
""".format(job_dir, input_lhe_location, output_eos, args.run_name, str(splitting), str(args.max), job_name)
)
os.system('cp '+submit_jdl_filename + ' ' + job_dir)

# submit
os.system('condor_submit '+submit_jdl_filename)

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_eos+'/"\n')  
  f.write('splitting = '+str(splitting)+'\n')  
os.system('mv job_info.py '+job_dir)
os.system('rm '+submit_jdl_filename)
