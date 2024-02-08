#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse

#griduser_id = (subprocess.check_output("voms-proxy-info --identity", shell=True).decode('utf-8')).split('/')[5][3:]
griduser_id = (subprocess.check_output("whoami").decode('utf-8')).strip()

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name of job for output and jobdir')
parser.add_argument('input_jobdir', help='jobdir of mini step')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
args = parser.parse_args()

if args.input_jobdir[-1] == '/': args.input_jobdir = args.input_jobdir[:-1]

# find mini step output area
loc = "."
dirs = []
for d in os.listdir(loc):
  if os.path.isdir(d) and d.startswith(args.input_jobdir):
    dirs.append(os.path.join(loc, d))
if not len(dirs)==1: raise SystemExit("Job Directory pattern returned zero or >1 directories.")
jobDir = dirs[0]
sys.path.append(jobDir)
import job_info as job
input_mini_location = job.output

input_directory = input_mini_location
output_directory = '/store/user/'+griduser_id+'/siggen/mergedmini/'+args.run_name+'/'
job_name = args.run_name+'_STEP_mergemini'
job_dir = 'Job_'+job_name
submit_file = 'merge_submit.jdl'
max_mat = args.max

with open(submit_file, 'w') as f:
  f.write(
'''universe = vanilla
initialdir = {}
error  = stdout/$(Cluster)_$(Process)_out.txt
output = stdout/$(Cluster)_$(Process)_out.txt
log    = log_$(Cluster).txt
executable = execute_STEP_mergemini.sh
transfer_input_files = ../cmssw_cfgs/merge_cfg.py
arguments = $(INPUT_EOS) root://cmseos.fnal.gov/$(OUTPUT_EOS) $(FINALFILE_NAME)
Notification = never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {}
OUTPUT_EOS = {}
JobBatchName = {}

queue INPUT_EOS, FINALFILE_NAME from queue.dat
'''.format(job_dir, max_mat, output_directory, job_name)
)

list_of_subdirs = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_directory)).split('\n')
with open('queue.dat', 'w') as f:
  for subdir in list_of_subdirs:
    finalfilename = subdir[subdir.rfind('/')+1:]
    f.write(subdir+' '+finalfilename+'\n')

os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout')
os.system('cp queue.dat '+job_dir)
os.system('cp '+submit_file+' '+job_dir)
os.system('condor_submit '+submit_file)

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_directory+'/"\n')  
os.system('mv job_info.py '+job_dir)
os.system('rm '+submit_file)
os.system('rm queue.dat')

