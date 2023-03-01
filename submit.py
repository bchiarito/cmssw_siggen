#!/usr/bin/env python3
import classad
import htcondor
import sys

# define submit file
sub = htcondor.Submit()
sub['executable'] = 'execute.sh'
#sub['arguments'] = '749 0.55 1'
sub['arguments'] = str(sys.argv[1]) + ' ' + str(sys.argv[2]) + ' ' + str(sys.argv[3])
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
sub['use_x509userproxy'] = 'true'
sub['should_transfer_files'] = 'YES'
sub['transfer_output_files'] = '""'
sub['initialdir'] = ''
sub['output'] = '$(Cluster)_$(Process)_out.txt'
sub['error'] = '$(Cluster)_$(Process)_out.txt'
sub['log'] = 'log_$(Cluster).txt'
sub['max_materialize'] = 1000

# schedd
collector = htcondor.Collector()
coll_query = collector.query(htcondor.AdTypes.Schedd, \
constraint='FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"',
projection=["Name", "MyAddress"]
)
schedd_ad = coll_query[0]
schedd = htcondor.Schedd(schedd_ad)

# submit
result = schedd.submit(sub)
print(result.cluster())
