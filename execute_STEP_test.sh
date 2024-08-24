#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "Here there are all the input arguments:"
echo $@
echo ""
xrdcp -s root://cmseos.fnal.gov//store/user/bchiari1/tarballs/madgraph/madgraph.tgz .
tar -xf madgraph.tgz
cd MG5_aMC_v2_6_0
pwd
ls
