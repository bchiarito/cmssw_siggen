#!/usr/bin/env python3
import sys
import os
import subprocess
import socket
import argparse

user_id = (subprocess.check_output("whoami").decode('utf-8')).strip()

parser = argparse.ArgumentParser()
parser.add_argument('input', help='job directory of STEP2')
parser.add_argument('runname', help='name for job directory and output area')
parser.add_argument('--output_base', '-o ', metavar='PATH', default='/cms/{}/eos/signal_gen_PhiToOmegaOmemga/STEP3'.format(user_id),  help='(hexcms only) base directory for output')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
parser.add_argument('--force', '-f', action='store_true', default=False, help=argparse.SUPPRESS)
args = parser.parse_args()

if args.input[-1] == '/': args.input = args.input[:-1]

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

if site == 'hexcms': local = True
else: local = False

# find mini step output area
loc = "."
dirs = []
for d in os.listdir(loc):
  if os.path.isdir(d) and d.startswith(args.input):
    dirs.append(os.path.join(loc, d))
if not len(dirs)==1: raise SystemExit("Job Directory pattern returned zero or >1 directories.")
jobDir = dirs[0]
sys.path.append(jobDir)
import job_info as job
input_mini_location = job.output
input_directory = input_mini_location
if site == 'cmslpc' : output_area = 'root://cmseos.fnal.gov//store/user/'+user_id+'/siggen/mergedmini/'+args.runname+'/'
if site == 'hexcms' :
    output_area = '/cms/chiarito/scratch2/'+args.runname
    os.system('mkdir -p ' + '/cms/chiarito/scratch2/'+args.runname)
job_name = args.runname+'_STEP3_mergemini'
job_dir = 'Job_'+job_name
submit_file = 'merge_submit.jdl'
max_mat = args.max
res = input('NOTICE: Using\n    {}\n    for output area, please ensure this is correct [Enter to continue / q to quit] '.format(output_area))
if not res == "": sys.exit()
print()

if site == 'hexcms': os_line = '+SingularityImage = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64"'
if site == 'cmslpc': os_line = '+DesiredOS = "SL7"'
if site == 'lxplus': os_line = '+WantOS = "el7"'
with open(submit_file, 'w') as f:
  f.write(
'''universe = vanilla
initialdir = {}
error  = stdout/$(Cluster)_$(Process)_out.txt
output = stdout/$(Cluster)_$(Process)_out.txt
log    = log_$(Cluster).txt
executable = execute_scripts/execute_STEP_mergemini.sh
transfer_input_files = ../cmssw_cfgs/merge_cfg.py
arguments = $(INPUT_LOC) $(OUTPUT_LOC) $(FINALFILE_NAME) {}
Notification = never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {}
OUTPUT_LOC = {}
JobBatchName = {}
{}

queue INPUT_LOC, FINALFILE_NAME from queue.dat
'''.format(job_dir, site, max_mat, output_area, job_name, os_line)
)

if site == 'cmslpc': list_of_subdirs = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_directory)).split('\n')
if site == 'hexcms': list_of_subdirs = (subprocess.getoutput("ls " + input_directory)).split('\n')
with open('queue.dat', 'w') as f:
  for subdir in list_of_subdirs:
    finalfilename = subdir[subdir.rfind('/')+1:]
    if site == 'hexcms': location = input_directory + subdir
    if site == 'cmslpc': location = subdir
    f.write(location+' '+finalfilename+'\n')

if os.path.isdir("./"+job_dir) and args.force: os.system('rm -rf ./' + job_dir)
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout')
os.system('cp queue.dat '+job_dir)
os.system('cp '+submit_file+' '+job_dir)
os.system('condor_submit '+submit_file)

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_area+'/"\n')  
os.system('mv job_info.py '+job_dir)
os.system('rm '+submit_file)
os.system('rm queue.dat')

