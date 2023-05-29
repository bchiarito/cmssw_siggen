#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse

griduser_id = (subprocess.check_output("voms-proxy-info --identity", shell=True).decode('utf-8')).split('/')[5][3:]

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for output eos area')
parser.add_argument('input_jobdir', help='job directory for splitlhe step')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
args = parser.parse_args()

job_name = args.run_name+'_STEP_mini'
job_dir = 'Job_'+job_name
output_eos = '/store/user/'+griduser_id+'/siggen/mini/'+args.run_name
submit_jdl_filename = 'submit_STEP_mini.jdl'
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
splitting = job.splitting

# make job directory
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout/')

# generate condor queue data
with open('queue.dat', 'w') as f:
  remote_split_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_lhe_location)).split('\n')
  for file in remote_split_lhes:
    x = file.rfind('_')
    y = file.rfind('.')
    c = file.rfind('C')
    name = file[:c-1]
    base = file[:x]
    num = file[x+1:y]
    count = file[c+1:x]
    b = base.rfind('/')
    out_tag = base[b+1:]
    output = os.path.join(output_eos, out_tag)
    f.write(base + ' ' + num + ' ' + output + '\n')
os.system('cp queue.dat ' + job_dir)

# make new submit file
with open(submit_jdl_filename, 'w') as f:
  f.write(
"""universe = vanilla
initialdir = {}
#use_x509userproxy = true
#x509userproxy         = $ENV(X509_USER_PROXY)
error  = stdout/$(Cluster)_$(Process)_out.txt
output = stdout/$(Cluster)_$(Process)_out.txt
log    = log_$(Cluster).txt
executable = execute_STEP_mini.sh
transfer_input_files = ../cmssw_cfgs/GEN_2018_cfg.py, ../cmssw_cfgs/SIM_2018_cfg.py, ../cmssw_cfgs/DIGIPremix_2018_cfg.py, ../cmssw_cfgs/HLT_2018_cfg.py, ../cmssw_cfgs/RECO_2018_cfg.py, ../cmssw_cfgs/MINIAOD_2018_cfg.py, ../cmssw_cfgs/Premix_RunIISummer20ULPrePremix-UL18_106X_upgrade2018_realistic_v11_L1v1-v2.list
arguments = $(FILE_NUM) 2018 90000054 -1 root://cmseos.fnal.gov/$(OUTPUT_EOS) $(INPUT_LHE)
Notification = never
request_memory = 4000
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {}
INPUT_LHE = $(LHEBASE)_$(FILE_NUM).lhe
JobBatchName = {}

queue LHEBASE, FILE_NUM, OUTPUT_EOS from queue.dat
""".format(job_dir, str(args.max), job_name)
)
os.system('cp '+submit_jdl_filename + ' ' + job_dir)

# submit
proc = subprocess.Popen('condor_submit '+submit_jdl_filename, stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()
out = ((out.decode('utf-8')).split('\n'))[1]
cluster = (out.split()[-1])[:-1]

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_eos+'/"\n')  
  f.write('cluster = '+cluster+'\n')  
  f.write('splitting = '+str(splitting)+'\n')
  f.write('resubmits = []\n')
os.system('mv job_info.py '+job_dir)
os.system('mv '+submit_jdl_filename+' '+job_dir)
os.system('rm queue.dat')
