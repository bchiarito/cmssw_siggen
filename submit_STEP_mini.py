#!/usr/bin/env python3
import sys
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for output eos area')
parser.add_argument('input_lhe', help='directory with .lhe files')
parser.add_argument('-s', '--split', type=int, default=1, help='split each lhe into this many subjobs')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
args = parser.parse_args()

job_dir = 'Job_'+args.run_name+'_STEP_mini'
input_lhe_location = args.input_lhe
splitting = args.split
output_eos = '/store/user/bchiari1/siggen/mini/'+args.run_name

submit_jdl_base = 'base_STEP_mini.jdl'

# make job directory
os.system('mkdir '+job_dir)
os.system('mkdir -p '+job_dir+'/stdout/')
os.system('mkdir -p '+job_dir+'/lhe/')

# split lhes and copy to job directory
for count, file in enumerate(os.listdir(input_lhe_location)):
  if not file.endswith('.lhe'): continue
  if splitting == 1:
    os.system('mv ' + os.path.join(input_lhe_location, file) + ' ' + job_dir + '/lhe/' + file[:-4]+'_C'+str(count)+'_0.lhe')
  elif splitting > 1:
    command = 'python splitLHE.py ' + os.path.join(input_lhe_location, file) + ' ' + file[:-4]+'_C'+str(count)+'_ '+str(splitting)
    os.system(command)
    for i in range(splitting):
      os.system('mv ' + file[:-4] + '_C'+str(count)+'_'+str(i)+'.lhe ' + job_dir+'/lhe/')

# generate condor queue data
with open('queue.dat', 'w') as f:
  for file in os.listdir(job_dir+'/lhe/'):
    x = file.rfind('_')
    y = file.rfind('.')
    c = file.rfind('C')
    name = file[:c-1]
    base = file[:x]
    num = file[x+1:y]
    count = file[c+1:x]
    output = os.path.join(output_eos, name)
    f.write(base + ' ' + num + ' ' + output + '\n')
os.system('cp queue.dat ' + job_dir)

# make new submit file
with open(submit_jdl_base, 'r') as f:
  filedata = f.read()
filedata = filedata.replace('__initialdir__', job_dir)
filedata = filedata.replace('__maxmat__', str(args.max))
with open('my_' + submit_jdl_base, 'w') as f:
  f.write(filedata)
os.system('cp my_'+submit_jdl_base + ' ' + job_dir)

# submit
os.system('condor_submit my_'+submit_jdl_base)

# cleanup
os.system('rm my_'+submit_jdl_base)
os.system('rm queue.dat')
