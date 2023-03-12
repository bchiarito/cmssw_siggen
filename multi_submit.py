#!/usr/bin/env python3
from __future__ import print_function
import classad
import htcondor
import sys
import os

# command line options
job_name = sys.argv[1]

# settings
parameters_file = 'options.txt'
phi_low = 100
phi_high = 6000
phi_step = 295 # 20 mass points
#phi_step = 59 # 100 mass points
omega_low = 0.25
omega_high = 10
omega_step = 0.4875 # 20 mass points
#omega_step = 0.0975 # 100 mass points
#omega_step = 0.0000975 # 100,000 mass points
num_per_mass_point = 10
max_materialize = 250

# create mass points file
with open(parameters_file, "w") as f:
  for phi_mass in range(phi_low, phi_high, phi_step):
    for omega_mass in range(int(omega_low*100000000), int(omega_high*100000000), int(omega_step*100000000)):
      line = ", ".join([str(phi_mass), str(omega_mass/100000000.0), str(num_per_mass_point)]) + '\n'
      f.write(line)

# submit file
sub = htcondor.Submit()
sub['executable'] = 'execute.sh'
sub['arguments'] = '$(PHI_MASS) $(OMEGA_MASS) $(NUM_EVENT) $(DEST)'
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
sub['use_x509userproxy'] = 'true'
sub['should_transfer_files'] = 'YES'
sub['transfer_output_files'] = '""'
sub['initialdir'] = job_name
sub['output'] = '$(Cluster)_$(Process)_out.txt'
sub['error'] = '$(Cluster)_$(Process)_out.txt'
sub['log'] = 'log_$(Cluster).txt'
sub['max_materialize'] = max_materialize
sub['DEST'] = '/store/user/bchiari1/siggen/lhe/' + job_name + '/'

# job directory
os.system('mkdir ' + job_name)

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
