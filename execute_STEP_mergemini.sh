#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "Here there are all the input arguments:"
echo $@

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc700
eval `scramv1 project CMSSW CMSSW_10_6_20`
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
cd ../..

cmsRun merge_cfg.py input=$1 outputFile=output.root maxEvents=10
xrdcp --nopbar output_numEvent10.root $2/$3.root
