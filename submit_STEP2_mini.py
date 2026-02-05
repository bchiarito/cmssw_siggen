#!/usr/bin/env python3
import sys
import os
import re
import subprocess
import argparse
import socket

griduser_id = (subprocess.check_output("whoami").decode('utf-8')).strip()

parser = argparse.ArgumentParser()
parser.add_argument('-r', '--runname', help='name for output eos area')
parser.add_argument('input_jobdir', help='job directory for splitlhe step')
parser.add_argument('-y', '--year', type=str, default="2018", choices=['2018','2017','2016'], help='year')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
decaytype = parser.add_mutually_exclusive_group()
decaytype.add_argument('--eta', action='store_true', default=True, help='')
decaytype.add_argument('--etaprime', action='store_true', default=True, help='')
parser.add_argument('--matching', action='store_true', default=True, help='')
parser.add_argument('--forced2prong', action='store_true', default=False, help='')
parser.add_argument('--test', '-t', action='store_true', default=False, help='')
parser.add_argument('--force', '-f', action='store_true', default=False, help='')
parser.add_argument('--localPremix', '-l', action='store_true', default=False, help='')
parser.add_argument("--proxy", "-p", default='', help="location of proxy file")

args = parser.parse_args()

if args.proxy == "": raise SystemExit('ERROR: No proxy specified')

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

if args.input_jobdir[-1] == '/': args.input_jobdir = args.input_jobdir[:-1]
if not args.runname: run_name = args.input_jobdir.replace('Job_','').replace('_STEP_splitlhe','')
else: run_name = args.runname
job_name = run_name+'_STEP_mini'
job_dir = 'Job_'+job_name
if site == 'cmslpc' : output_eos = '/store/user/'+griduser_id+'/siggen/mini/'+run_name
if site == 'hexcms' : output_eos = '/cms/chiarito/scratch/'+run_name
submit_jdl_filename = 'submit_STEP_mini.jdl'

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
numEvents = job.numEventsEach

# make job directory
if os.path.isdir("./"+job_dir) and args.force: os.system('rm -rf ./' + job_dir)
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout/')

def ls_sort(item):
    match = re.search(r'\d+(?=\.[a-zA-Z]+)', item)
    if match: return int(match.group())
    else: return float('inf')

# generate condor queue data
with open('queue.dat', 'w') as f:
  if site == 'cmslpc': input_split_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + input_lhe_location)).split('\n')
  if site == 'hexcms': input_split_lhes = (subprocess.getoutput("ls " + input_lhe_location)).split('\n')
  input_split_lhes.sort(key=ls_sort)

  for file in input_split_lhes:
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
    if site == 'hexcms': base = "file:" + input_lhe_location + '/' + base
    f.write(base + ' ' + num + ' ' + output + '\n')
os.system('cp queue.dat ' + job_dir)

if args.eta: decaytype = 1
elif args.etaprime: decaytype = 2
else: decaytype =1

if args.forced2prong: forcing = 1
else: forcing = 0

if args.matching: matching = 1
else: matching = 0

# make new submit file
if site == 'hexcms': prefix = ""
if site == 'cmslpc': prefix = "root://cmseos.fnal.gov/"

with open(submit_jdl_filename, 'w') as f:
  numEvents=5
  f.write(
"""universe = vanilla
initialdir = {0:}
error  = stdout/$(Cluster)_$(Process)_out.txt
output = stdout/$(Cluster)_$(Process)_out.txt
log    = log_$(Cluster).txt
executable = execute_STEP_mini.sh
transfer_input_files = ../cmssw_cfgs/GEN_{1:}_cfg.py, ../cmssw_cfgs/SIM_{1:}_cfg.py, ../cmssw_cfgs/DIGIPremix_{1:}_cfg.py, ../cmssw_cfgs/HLT_{1:}_cfg.py, ../cmssw_cfgs/RECO_{1:}_cfg.py, ../cmssw_cfgs/MINIAOD_{1:}_cfg.py, ../cmssw_cfgs/Premix_{1:}.list
arguments = $(FILE_NUM) {1:} 90000054 {8:} {7:}$(OUTPUT_EOS) $(INPUT_LHE) {2:} {3:} {4:} {9:}
Notification = never
request_memory = 4000
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
max_materialize = {5:}
INPUT_LHE = {7:}$(LHEBASE)_$(FILE_NUM).lhe
JobBatchName = {6:}
""".format(job_dir, args.year, decaytype, forcing, matching, str(args.max), job_name, prefix, numEvents, int(args.localPremix)))
  if site == 'hexcms':
    f.write('+SingularityImage = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64"\n')
  if site == 'cmslpc':
    f.write('+DesiredOS = "SL7"\n')
  if site == 'hexcms':
    f.write('x509userproxy = ../{}\n'.format(args.proxy))
    #f.write('x509userproxy = x509up_u756\n')
  f.write("\nqueue LHEBASE, FILE_NUM, OUTPUT_EOS from queue.dat\n")

os.system('cp '+submit_jdl_filename + ' ' + job_dir)

if args.test:
  os.system('cat '+submit_jdl_filename)
  os.system('rm '+submit_jdl_filename)
  os.system('echo ""')
  os.system('cat queue.dat')
  os.system('rm queue.dat')
  os.system('rm -rf '+job_dir)
  exit()

# submit
proc = subprocess.Popen('condor_submit '+submit_jdl_filename, stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()
out = ((out.decode('utf-8')).split('\n'))[1]
cluster = (out.split()[-1])[:-1]
print(cluster)

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "'+output_eos+'/"\n')  
  f.write('cluster = '+cluster+'\n')  
  f.write('splitting = '+str(splitting)+'\n')
  f.write('resubmits = []\n')
os.system('mv job_info.py '+job_dir)
os.system('mv '+submit_jdl_filename+' '+job_dir)
os.system('rm queue.dat')
