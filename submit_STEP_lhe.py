#!/usr/bin/env python3
from __future__ import print_function
import classad
import htcondor
import sys
import os
import subprocess

griduser_id = (subprocess.check_output("whoami").decode('utf-8')).strip()

# command line options
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for job directory and output directory')
parser.add_argument('phi_num', type=int, help='number of steps in phi dimension')
parser.add_argument('omega_num', type=int, help='number of steps in omega dimension')
parser.add_argument('ev_per_point', type=int, help='number of events at each mass point')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
parser.add_argument('--extra', action='store_true', default=False, help='turn on generation of one extra jet')
parser.add_argument('--omega_low', type=float, help='replace default (0.4 GeV) omega low')
parser.add_argument('--omega_high', type=float, help='replace default (10 GeV) omega high')
parser.add_argument('--phi_low', type=int, help='replace default (100 GeV) phi low')
parser.add_argument('--phi_high', type=int, help='replace default (5000 GeV) phi high')
args = parser.parse_args()

# settings
job_name = args.run_name+'_STEP_lhe'
job_dir = 'Job_'+job_name
job_output = args.run_name
phi_num = args.phi_num
omega_num = args.omega_num
num_per_mass_point = args.ev_per_point
max_materialize = args.max
parameters_file = 'options.txt'
phi_low = 100
phi_high = 5000
omega_low = 0.4
omega_high = 10
if args.omega_low: omega_low = args.omega_low
if args.omega_high: omega_high = args.omega_high
if args.phi_low: phi_low = args.phi_low
if args.phi_high: phi_high = args.phi_high
if omega_num == 1: omega_step = omega_high
else: 
  omega_step = round((omega_high - omega_low)/(omega_num-1),4)
  omega_high += omega_step
if phi_num == 1: phi_step = phi_high
else: 
  phi_step = int(round((phi_high - phi_low)/(phi_num-1)))
  phi_high += phi_step

# summary
print("Phi, omega")
print("---")
for phi_mass in range(int(phi_low), int(phi_high), int(phi_step)):
    for omega_mass in range(int(omega_low*100000000), int(omega_high*100000000), int(omega_step*100000000)):
        line = ", ".join([str(phi_mass), str(omega_mass/100000000.0)])
        print(line)
print("---")
if phi_num == 1: print('Phi =', phi_low)
else: print('Phi from', phi_low, 'to', phi_high-phi_step, 'in steps of', phi_step)
if omega_num == 1: print('omega =', omega_low)
else: print('omega from', omega_low, 'to', omega_high-omega_step, 'in steps of', omega_step)
print("{:,} events per mass point".format(num_per_mass_point))
response = input("\nContinue? [Enter] to proceed, q to quit: ")
if response == 'q':
  print("Quitting.")
  sys.exit()

# create mass points file
with open(parameters_file, "w") as f:
  for phi_mass in range(int(phi_low), int(phi_high), int(phi_step)):
    for omega_mass in range(int(omega_low*100000000), int(omega_high*100000000), int(omega_step*100000000)):
      line = ", ".join([str(phi_mass), str(omega_mass/100000000.0), str(num_per_mass_point)]) + '\n'
      f.write(line)

# submit file
sub = htcondor.Submit()
sub['executable'] = 'execute_STEP_lhe.sh'
sub['arguments'] = '$(PHI_MASS) $(OMEGA_MASS) $(NUM_EVENT) $(DEST) {}'.format(str(args.extra))
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
sub['use_x509userproxy'] = 'true'
sub['should_transfer_files'] = 'YES'
sub['transfer_output_files'] = '""'
sub['initialdir'] = job_dir
sub['output'] = '$(Cluster)_$(Process)_out.txt'
sub['error'] = '$(Cluster)_$(Process)_out.txt'
sub['log'] = 'log_$(Cluster).txt'
sub['max_materialize'] = max_materialize
sub['DEST'] = '/store/user/'+griduser_id+'/siggen/lhe/' + job_output + '/'
sub['request_memory'] = 8000
sub['JobBatchName'] = job_name
#sub['+DesiredOS'] = '"SL7"'

# job directory
os.system('mkdir ' + job_dir)
os.system('cp '+parameters_file+' '+job_dir+'/')

# schedd
collector = htcondor.Collector()
coll_query = collector.query(htcondor.AdTypes.Schedd, \
constraint='FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"',
projection=["Name", "MyAddress"]
)
schedd_ad = coll_query[0]
schedd = htcondor.Schedd(schedd_ad)

# submit
iterator = sub.itemdata("queue PHI_MASS, OMEGA_MASS, NUM_EVENT from " + parameters_file)
result = schedd.submit(sub, itemdata = iterator)
cluster_id = result.cluster()
print('ClusterID', cluster_id)

# finish
with open('job_info.py', 'w') as f:
  f.write('output = "/store/user/'+griduser_id+'/siggen/lhe/'+job_output+'/"\n')
os.system('mv job_info.py '+job_dir)
os.system('rm '+parameters_file)
