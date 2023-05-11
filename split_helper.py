#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('input_loc', help='')
parser.add_argument('output_loc', help='')
parser.add_argument('run_name', help='')
parser.add_argument('-s', '--split', type=int, default=1, help='split each lhe into this many subjobs')
args = parser.parse_args()

splitting = args.split

list_of_lhes = (subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + args.input_loc)).split('\n')
print(list_of_lhes)
for count, file in enumerate(list_of_lhes):
  if not file.endswith('.lhe'): continue
  print('xrdcp root://cmseos.fnal.gov/'+file+' .')
  os.system('xrdcp root://cmseos.fnal.gov/'+file+' .')
  local_lhe_filename = os.path.basename(file)
  if splitting == 1:
    os.system('xrdcp ' + local_lhe_filename + ' root://cmseos.fnal.gov/'+args.output_loc+'/'+local_lhe_filename)
  elif splitting > 1:
    command = 'python splitLHE.py ' + local_lhe_filename + ' ' + local_lhe_filename[:-4]+'_C'+str(count)+'_ '+str(splitting)
    print(command)
    os.system(command)
    for i in range(splitting):
      os.system('xrdcp ' + local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe ' + 'root://cmseos.fnal.gov/'+args.output_loc+'/'+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe')
      os.system('rm '+local_lhe_filename[:-4]+'_C'+str(count)+'_'+str(i)+'.lhe')
  os.system('rm '+local_lhe_filename)

print('All finished')
