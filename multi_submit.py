#!/usr/bin/env python3
from __future__ import print_function
import classad
import htcondor
import sys
import os

job_name = sys.argv[1]
parameters_file = 'options.txt'

# define submit file
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
sub['max_materialize'] = 1
sub['DEST'] = '/store/user/bchiari1/lhe/' + job_name + '/'

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
