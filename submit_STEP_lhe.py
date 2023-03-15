#!/usr/bin/env python3
from __future__ import print_function
import classad
import htcondor
import sys
import os

# command line options
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('run_name', help='name for job directory and output directory')
parser.add_argument('phi_num', type=int, help='number of steps in phi dimension')
parser.add_argument('omega_num', type=int, help='number of steps in omega dimension')
parser.add_argument('ev_per_point', type=int, help='number of events at each mass point')
parser.add_argument('-m', '--max', type=int, default=250, help='max_materialize (default 250)')
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
phi_high = 6000
omega_low = 0.25
omega_high = 10
phi_step = (phi_high - phi_low)/phi_num
omega_step = (omega_high - omega_low)/omega_num

# summary
print('Phi from', phi_low, 'to', phi_high, 'in steps of', phi_step)
print('omega from', omega_low, 'to', omega_high, 'in steps of', omega_step)
response = input("Continue? [Enter] to proceed, q to quit: ")
if response == 'q':
  print("Quitting.")
  sys.exit()

# create mass points file
with open(parameters_file, "w") as f:
  for phi_mass in range(int(phi_low*100), int(phi_high*100), int(phi_step*100)):
    for omega_mass in range(int(omega_low*100000000), int(omega_high*100000000), int(omega_step*100000000)):
      line = ", ".join([str(phi_mass/100.0), str(omega_mass/100000000.0), str(num_per_mass_point)]) + '\n'
      f.write(line)

# submit file
sub = htcondor.Submit()
sub['executable'] = 'execute_STEP_lhe.py'
sub['arguments'] = '$(PHI_MASS) $(OMEGA_MASS) $(NUM_EVENT) $(DEST)'
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
sub['DEST'] = '/store/user/bchiari1/siggen/lhe/' + job_output + '/'

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
  f.write('output = "/store/user/bchiari1/siggen/lhe/'+job_output+'/"\n')
os.system('mv job_info.py '+job_dir)
os.system('rm '+parameters_file)
