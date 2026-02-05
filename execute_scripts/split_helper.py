#!/usr/bin/env python3
import sys
import os
import subprocess
import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('input_loc', help='')
parser.add_argument('output_loc', help='')
parser.add_argument('run_name', help='')
parser.add_argument('-s', '--split', type=int, default=1, help='split each lhe into this many subjobs')
parser.add_argument('--local', type=int, default=1, help='')
args = parser.parse_args()

splitting = args.split

local = bool(args.local)

if local:
  list_of_lhes = (subprocess.getoutput("ls " + args.input_loc)).split('\n')
else:
  list_of_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + args.input_loc)).split('\n')

if local: os.system('mkdir -p ' + args.output_loc)

print(list_of_lhes)
for count, file in enumerate(list_of_lhes):
  if not file.endswith('.lhe'): continue
  if local: copyin = 'cp '+ args.input_loc + '/' + file + ' .'
  else: copyin = 'xrdcp root://cmseos.fnal.gov/'+file+' .'
  print(copyin)
  os.system(copyin)
  local_lhe_filename = os.path.basename(file)
  if splitting == 1:
    if local:stageout = 'cp ' + local_lhe_filename + ' '+args.output_loc+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_0.lhe'
    else: stageout = 'xrdcp ' + local_lhe_filename + ' root://cmseos.fnal.gov/'+args.output_loc+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_0.lhe'
    os.system(stageout)
  elif splitting > 1:
    split_command = 'python3 splitLHE.py ' + local_lhe_filename + ' ' + local_lhe_filename[:-4]+'_C'+str(count)+'_ '+str(splitting)
    print(split_command)
    os.system(split_command)
    for i in range(splitting):
      if local:
        stageout = 'cp ' + local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe ' + ' '+args.output_loc+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe'
      else: stageout = 'xrdcp ' + local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe ' + 'root://cmseos.fnal.gov/'+args.output_loc+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe'
      os.system(stageout)
      os.system('rm '+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe')
  os.system('rm '+local_lhe_filename)

print('All finished')
