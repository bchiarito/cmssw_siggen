#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "Here there are all the input arguments:"
echo $@
echo ""
export INITIAL_DIR=$(pwd)
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc820
scramv1 project CMSSW CMSSW_10_6_27
cd CMSSW_10_6_27/src
eval `scramv1 runtime -sh`
cd $INITIAL_DIR
which python
python --version
which python3
python3 --version
which gfortran
gfortran --version
xrdcp -s root://cmseos.fnal.gov//store/user/bchiari1/tarballs/madgraph/madgraph.tgz .
tar -xf madgraph.tgz
cd MG5_aMC_v2_6_0
pwd
ls
echo "\n=== now running ===\n"
python my_utilities/auto_run.py $1 $2 $3
echo "\n=== print contents of my_events ===\n"
ls my_events/
echo "\n=== now copy ===\n"
xrdcp my_events/$(ls my_events/) root://cmseos.fnal.gov/$4/$(ls my_events/)
