#!/usr/bin/env python3
import sys
import os
import re
import subprocess
import socket
import argparse

griduser_id = (subprocess.check_output("whoami").decode('utf-8')).strip()
#griduser_id = (subprocess.check_output("voms-proxy-info --identity", shell=True).decode('utf-8')).split('/')[5][3:]

parser = argparse.ArgumentParser()
parser.add_argument('input', help='directory with lhes or job directory from STEP0')
parser.add_argument('runname', help='name for job directory and output area')
parser.add_argument('-s', '--split', metavar='N', type=int, default=1, help='split each lhe into this many subjobs')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
parser.add_argument('-o', '--omitNumEvents', default=False, action='store_true', help='debug option')
args = parser.parse_args()

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

if site == 'hexcms': local = True
else: local = False

if args.input[-1] == '/': args.input = args.input[:-1]
#if not args.runname: run_name = args.input.replace('Job_','').replace('_STEP_lhe','')
#else: run_name = args.runname
job_name = args.runname+'_STEP_splitlhe'
job_dir = 'Job_'+job_name
splitting = args.split
if local: output_area = '/cms/chiarito/eos/siggen/splitlhe/'+args.runname
else: output_area = '/store/user/'+griduser_id+'/siggen/splitlhe/'+args.runname
submit_jdl_filename = 'submit_STEP_splitlhe.jdl'

# find input lhe area
if local: 
    input_lhe_location = args.input
else:
    loc = "."
    dirs = []
    for d in os.listdir(loc):
      if os.path.isdir(d) and d.startswith(args.input):
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
executable = execute_scripts/execute_STEP_splitlhe.sh
transfer_input_files = ../execute_scripts/split_helper.py, ../execute_scripts/splitLHE.py
arguments = {} {} {} {}
Notification = never
request_memory = 4000
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {}
JobBatchName = {}

queue
""".format(job_dir, input_lhe_location, output_area, args.runname, str(splitting), str(args.max), job_name)
)
os.system('cp '+submit_jdl_filename + ' ' + job_dir)

# submit
os.system('condor_submit '+submit_jdl_filename)

# determine number of events
if not args.omitNumEvents:
    if local: list_of_lhes = (subprocess.getoutput("ls " + input_lhe_location)).split('\n')
    else: list_of_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_lhe_location)).split('\n')
    testfile = list_of_lhes[0]
    if local: copyin = 'cp '+ input_lhe_location + '/' + testfile + ' .'
    else: copyin = 'xrdcp root://cmseos.fnal.gov/'+ testfile +' .'
    os.system(copyin)
    with open(testfile) as fin:
        eventNum = 0
        init = False
        inFooter = False
        for line in fin:
          if re.match(r"[^#]*</LesHouchesEvents>",line):
            inFooter = True
          elif inFooter:
            pass
          elif init:  
            if re.match(r"[^#]*</event>",line):
              eventNum += 1
          elif re.match(r"[^#]*</init>",line):
            init = True 
        eventsTotal = eventNum
    os.system('rm ' + testfile)
    numEventsEach = int(round(float(eventsTotal)/args.split) + 1)
else:
    numEventsEach = -1

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_area+'/"\n')  
  f.write('splitting = '+str(splitting)+'\n')  
  f.write('numEventsEach = '+str(numEventsEach)+'\n')
os.system('mv job_info.py '+job_dir)
os.system('rm '+submit_jdl_filename)
