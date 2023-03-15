#!/usr/bin/env python3
import os
import sys
import argparse
import time
import socket
# get site

parser = argparse.ArgumentParser(description="")
parser.add_argument('jobdir', help='job directory of the lhe step')
parser.add_argument('dest', default='/uscms/home/bchiari1/nobackup/', help='local storage destination')
args = parser.parse_args()

destination = args.dest
os.system('mkdir -p '+destination)

loc = "."
dirs = []
for d in os.listdir(loc):
  if os.path.isdir(d) and d.startswith(args.jobdir):
    dirs.append(os.path.join(loc, d))

print(dirs,'\n')

# import job
for i, jobDir in enumerate(dirs):
  sys.path.append(jobDir)
  import job_info as job
  output_area = job.output
  del job
  sys.path.pop()
  sys.modules.pop('job_info')
  print('xrdcp -r --nopbar root://cmseos.fnal.gov/'+output_area+' '+destination+''+os.path.basename(jobDir)+'_output')
  temp = os.getcwd()
  os.chdir(destination)
  os.system('mkdir '+os.path.basename(jobDir)+'_output')
  os.system('xrdcp -r --nopbar root://cmseos.fnal.gov/'+output_area+' '+destination+''+os.path.basename(jobDir)+'_output')
  os.chdir(temp)
