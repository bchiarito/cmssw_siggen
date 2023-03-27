#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for output eos area')
parser.add_argument('input_lhe', help='eos area with .lhe files')
parser.add_argument('-s', '--split', type=int, default=1, help='split each lhe into this many subjobs')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
args = parser.parse_args()

job_name = args.run_name+'_STEP_mini'
job_dir = 'Job_'+job_name
input_lhe_location = args.input_lhe
splitting = args.split
output_eos = '/store/user/bchiari1/siggen/mini/'+args.run_name

submit_jdl_filename = 'submit_STEP_mini.jdl'

# make job directory
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout/')
os.system('mkdir -p '+job_dir+'/lhe/')

# split lhes and copy to job directory
list_of_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_lhe_location)).split('\n')
#print(list_of_lhes)
for count, file in enumerate(list_of_lhes):
  if not file.endswith('.lhe'): continue
  #print('xrdcp root://cmseos.fnal.gov/'+file+' .')
  os.system('xrdcp root://cmseos.fnal.gov/'+file+' .')
  local_lhe_filename = os.path.basename(file)
  if splitting == 1:
    print('xrdcp ' + local_lhe_filename + ' root://cmseos.fnal.gov/'+input_lhe_location+'/split-'+args.run_name+'/'+local_lhe_filename)
    os.system('xrdcp ' + local_lhe_filename + ' root://cmseos.fnal.gov/'+input_lhe_location+'/split-'+args.run_name+'/'+local_lhe_filename)
  elif splitting > 1:
    command = 'python splitLHE.py ' + local_lhe_filename + ' ' + local_lhe_filename[:-4]+'_C'+str(count)+'_ '+str(splitting)
    print(command)
    os.system(command)
    for i in range(splitting):
      os.system('xrdcp ' + local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe ' + 'root://cmseos.fnal.gov/'+input_lhe_location+'/split-'+args.run_name+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe')
      os.system('rm '+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe')
  os.system('rm '+local_lhe_filename)

# generate condor queue data
with open('queue.dat', 'w') as f:
  remote_split_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_lhe_location+'/split-'+args.run_name)).split('\n')
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
os.system('condor_submit '+submit_jdl_filename)

# cleanup
os.system('rm '+submit_jdl_filename)
os.system('rm queue.dat')
