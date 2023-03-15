#!/bin/bash
#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ">>> Here there are all the input arguments:"
echo $@
source /cvmfs/cms.cern.ch/cmsset_default.sh  ## if a tcsh script, use .csh instead of .sh
export SCRAM_ARCH=slc7_amd64_gcc700

pwd
ls -l

printf "\n\n"
printf "\n$1 : file number"
printf "\n$2 : year (2016/2017/2018)"
printf "\n$3 : hadronizer"
printf "\n$4 : numEvents to run over"
printf "\n$5 : outputDir to xrdcp to\n"
printf "\n$6 : name of lhe\n"

#GEN, SIM, DIGI
eval `scramv1 project CMSSW CMSSW_10_6_20`
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
cd ${_CONDOR_SCRATCH_DIR}
pwd

printf "\n\nDoing LHE > GEN\n"
cmsRun GEN_$2_cfg.py inputFile=file:$6 hadronizer=$3 numEvents=$4
ls

printf "\n\nDoing GEN > SIM\n"
cmsRun SIM_$2_cfg.py
ls

# xrdcp SIM.root $5/gensim_$1.root
# xrdcp $5/gensim_$1.root SIM.root

printf "\n\nDoing SIM > DIGI\n"
cmsRun DIGIPremix_$2_cfg.py
ls

#HLT
eval `scramv1 project CMSSW CMSSW_10_2_16_UL`
cd CMSSW_10_2_16_UL/src
eval `scramv1 runtime -sh`
cd ${_CONDOR_SCRATCH_DIR}
pwd

printf "\n\nDoing DIGI > HLT\n"
cmsRun HLT_$2_cfg.py
ls

#RECO, MINI
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
cd ${_CONDOR_SCRATCH_DIR}
pwd

printf "\n\nDoing HLT > RECO\n"
cmsRun RECO_$2_cfg.py
ls

# xrdcp RECO.root $5/reco_$1.root
# xrdcp $5/reco_$1.root RECO.root

printf "\n\nDoing RECO > MINI\n"
cmsRun MINIAOD_$2_cfg.py outputFile=miniAOD_$1.root
ls

#Cleaning up
xrdcp --nopbar miniAOD_$1.root $5/miniAOD_$1.root
rm $6 GEN.root SIM.root DIGIPremix.root HLT.root RECO.root miniAOD_$1.root
