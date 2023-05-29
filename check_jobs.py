#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import re
import json
import time
from calendar import timegm
from datetime import datetime, timedelta, date
import datetime
import socket

submit_filename = 'submit_STEP_mini.jdl'

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

# import condor modules
try:
  import classad
  import htcondor
except ImportError as err:
  if site == 'hexcms':
    raise err
  if site == 'cmslpc':
    print('ERROR: Could not import classad or htcondor. Verify that python is default and not from cmssw release (do not cmsenv).')
    raise err

# argparse
parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the job directory")
parser.add_argument("-s", "--summary", default=False, action="store_true",help="do not print one line per job, instead summarize number of jobs with each status type")
parser.add_argument("--onlyFinished", default=False, action="store_true",help="ignore 'running' and 'submitted' job Ids")
parser.add_argument("--notFinished", default=False, action="store_true",help="ignore 'finished' job Ids")
parser.add_argument("--onlyError", default=False, action="store_true",help="ignore 'running', 'submitted', and 'finished, job Ids")
parser.add_argument("--onlyResubmits", default=False, action="store_true",help="only job Ids resubmitted at least once")
parser.add_argument("--group", default=False, action="store_true",help="group according to job Id status, instead of numerical order")
parser.add_argument("--aborted", "-a", default = False, action="store_true", help="print a string of 'aborted' job IDs for condor_resubmit.py")
parser.add_argument("--noOutput", default = False, action="store_true", help="print a string of 'fin w/o output' job IDs for condor_resubmit.py")
parser.add_argument("-r", "--resubmit", type=str, help="list of job IDs to resubmit")
args = parser.parse_args()

# import job
sys.path.append(args.jobDir)
import job_info as job
output_area = job.output
cluster = str(job.cluster)
splitting = int(job.splitting)
with open(args.jobDir+'queue.dat') as q:
  procs = len(q.readlines())

def itoMP(i):
  return int((i - (i % splitting))/splitting)

def CStoproc(C, S):
  return int(C*splitting + S)

# get the schedd
if site == 'cmslpc':
  collector = htcondor.Collector()
  coll_query = collector.query(htcondor.AdTypes.Schedd, \
  constraint='FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"',
  projection=["Name", "MyAddress"]
  )
  schedd_ad = coll_query[0]
schedd = htcondor.Schedd(schedd_ad)

# loop over logs and get proc statuses
jobInfos = [{} for i in range(procs)]
regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
wait_command = 'condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+cluster+'.txt'
try:
  wait_output = subprocess.check_output(wait_command, shell=True)
except subprocess.CalledProcessError as e:
  wait_output = e.output
matches = re.finditer(regex, wait_output.decode('utf-8'), re.MULTILINE | re.DOTALL)
for match in matches:
  block = json.loads(match.group(0))
  date = time.strptime(str(block['EventTime']), '%Y-%m-%dT%H:%M:%S')
  t = timegm(date)
  if block['MyType'] == 'SubmitEvent':
    jobInfos[int(block['Proc'])]['resubmitted'] = 0
    jobInfos[int(block['Proc'])]['start_time'] = date
    jobInfos[int(block['Proc'])]['status'] = 'submitted'
  if block['MyType'] == 'ExecuteEvent':
    jobInfos[int(block['Proc'])]['status'] = 'running'
    jobInfos[int(block['Proc'])]['reason'] = ''
  if block['MyType'] == 'JobReleaseEvent':
    jobInfos[int(block['Proc'])]['status'] = 'rereleased'
  if block['MyType'] == 'JobTerminatedEvent':
    jobInfos[int(block['Proc'])]['end_time'] = date
    if block['TerminatedNormally']:
      jobInfos[int(block['Proc'])]['status'] = 'finished'
    else:
      jobInfos[int(block['Proc'])]['status'] = 'failed'
  if block['MyType'] == 'ShadowExceptionEvent':
    jobInfos[int(block['Proc'])]['end_time'] = date
    jobInfos[int(block['Proc'])]['status'] = 'exception!'
    jobInfos[int(block['Proc'])]['reason'] = block['Message']
  if block['MyType'] == 'FileTransferEvent' and block['Type'] == 6:
    jobInfos[int(block['Proc'])]['end_time'] = date
    jobInfos[int(block['Proc'])]['status'] = 'transferred'
  if block['MyType'] == 'JobAbortedEvent':
    jobInfos[int(block['Proc'])]['end_time'] = date
    jobInfos[int(block['Proc'])]['reason'] = block['Reason']
    jobInfos[int(block['Proc'])]['status'] = 'aborted'
  if block['MyType'] == 'JobHeldEvent':
    jobInfos[int(block['Proc'])]['end_time'] = date
    jobInfos[int(block['Proc'])]['reason'] = block['HoldReason']    
    jobInfos[int(block['Proc'])]['status'] = 'held'

# look in output area for output files
subdirs = (subprocess.getoutput('eos root://cmseos.fnal.gov ls '+output_area)).split('\n')
for dir in subdirs:
  #print(dir)
  ls_output = subprocess.getoutput('eos root://cmseos.fnal.gov ls -lh '+output_area+'/'+dir)
  for line in ls_output.split('\n'):
    l = line.split()
    if len(l) <= 6: continue
    try:
      fi = l[len(l)-1]
      size = l[4]+' '+l[5]
      u = fi.rfind('_')
      d = fi.rfind('.')
      S = int(fi[u+1:d]) # from outputfile
      if dir[-1] == '/': dir = dir[:-1]
      c = dir.rfind('C')
      C = int(dir[c+1:])
      proc = CStoproc(C, S)
      if proc >= procs: continue
      if proc < 0: continue
      jobInfos[proc]['size'] = size
    except (IndexError, ValueError):
      print("WARNING: got IndexError or ValueError, may want to check output area directly with (eos) ls.")
      continue

# debug jobInfos dictionary
#for i, jobInfo in enumerate(jobInfos):
#  print("proc", i)
#  for item in jobInfo:
#    print("  ", item, "=", jobInfo[item])

# display information
print("Results for ClusterId", cluster)
for count, resubmit_cluster in enumerate(job.resubmits):
  print("  Resubmit", count+1, "ClusterId", resubmit_cluster[0])
if not args.summary and not args.aborted and not args.noOutput:
  print(' {:<5}| {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {}'.format(
       'Proc', 'MP', 'Status', 'Resubs', 'Wall Time', 'Output File', 'Msg'))
if args.summary: summary = {}
lines = []
aborted_jobs = []
noOutput_jobs = []
for i, jobInfo in enumerate(jobInfos):
  status = jobInfo.get('status','unsubmitted')
  reason = jobInfo.get('reason','')
  reason = reason[0:80]
  if status == 'aborted':
    aborted_jobs.append(i)
  if 'start_time' in jobInfo and 'end_time' in jobInfo:
    totalTime = str(datetime.timedelta(seconds = timegm(jobInfo['end_time']) - timegm(jobInfo['start_time'])))
  elif 'start_time' in jobInfo:
    totalTime = str(datetime.timedelta(seconds = timegm(time.localtime()) - timegm(jobInfo['start_time'])))
  elif 'end_time' in jobInfo:
    totalTime = time.strftime('%m/%d %H:%M:%S', jobInfo['end_time']) + " (end)"
  else:
    totalTime = ''
  size = jobInfo.get('size', "")
  if status=='finished' and size=='':
    status = 'fin w/o output'
    noOutput_jobs.append(i)
  if args.onlyFinished and (status=='submitted' or status=='running' or status=='unsubmitted'): continue
  if args.onlyError and (status=='submitted' or status=='running' or status=='finished' or status=='unsubmitted'): continue
  if args.notFinished and (status=='finished'): continue
  resubs = jobInfo.get('resubmitted', '')
  if resubs == 0: resubs = ''
  if args.onlyResubmits and resubs == '': continue  
  lines.append(' {:<5}| {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {:<60.60}'.format(
         str(i), str(itoMP(i)), str(status), str(resubs), str(totalTime), str(size), str(reason)
  ))
  if args.summary:
    if status in summary: summary[status] += 1
    else: summary[status] = 1

if not args.summary and not args.aborted and not args.noOutput:
  if args.group:
    def status(line):
      return (line.split('|'))[1]
    lines = sorted(lines, key=status, reverse=True)
  for line in lines:
    print(line)

if args.summary:
  total = 0
  for key in summary:
    total += summary[key]
  print('{:<15} | {}'.format('Status', 'Job Ids ({} total)'.format(total)))
  for status in summary:
    print('{:<15} | {}'.format(str(status), str(summary[status])))

if args.aborted and len(aborted_jobs) != 0:
  x = 0
  while x < len(aborted_jobs):
      if x == len(aborted_jobs) - 1:
          sys.stdout.write(str(aborted_jobs[x]))
          break
      sys.stdout.write(str(aborted_jobs[x]) + ",")
      x += 1
  sys.stdout.write("\n")
elif args.aborted:
  print("No jobs were aborted")

if args.noOutput and len(noOutput_jobs) != 0:
  x = 0
  while x < len(noOutput_jobs):
      if x == len(noOutput_jobs) - 1:
          sys.stdout.write(str(noOutput_jobs[x]))
          break
      sys.stdout.write(str(noOutput_jobs[x]) + ",")
      x += 1
  sys.stdout.write("\n")
elif args.noOutput:
  print("No jobs finished without output")

# make list of procs to resubmit
if not args.resubmit: raise SystemExit()
procs = []
for a, b in re.findall(r'(\d+)-?(\d*)', args.resubmit):
  procs.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
procs_string = ''
for p in procs:
  procs_string += str(p)+','
procs_string = procs_string[:-1]

# create new submit jdl
batchName = "resub_for_"+str(job.cluster)
with open(args.jobDir+'/'+submit_filename) as f:
  with open('submit_resubmit.jdl', 'w') as s:
    submit_string = f.readlines()
    queue_statment = submit_string.pop()
    submit_string.append("\nJobBatchName = " + batchName+"\n")
    submit_string.append('\nnoop_job = !stringListMember("$(Process)","'+procs_string+'")\n')
    submit_string.append(queue_statment+'\n')
    s.writelines(submit_string)
print(submit_string)
os.system('cp '+args.jobDir+'/queue.dat .')

# move/delete old output

# submit the job
proc = subprocess.Popen('condor_submit submit_resubmit.jdl', stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()
out = ((out.decode('utf-8')).split('\n'))[1]
cluster = (out.split()[-1])[:-1]
print(cluster)

# update job_info.py
with open(args.jobDir+'/job_info.py', 'a') as f:
  f.write("resubmits.append(('"+str(cluster)+"',["+procs_string+"]))\n")
os.system('mv submit_resubmit.jdl '+args.jobDir+'/'+'resubmit_'+str(cluster)+'.jdl')
os.system('rm queue.dat')
