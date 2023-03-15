import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
import subprocess

options = VarParsing ('analysis')

# add a list of strings for events to process
options.register ('eventsToProcess',
          '',
          VarParsing.multiplicity.list,
          VarParsing.varType.string,
          "Events to process")
options.register ('maxSize',
          0,
          VarParsing.multiplicity.singleton,
          VarParsing.varType.int,
          "Maximum (suggested) file size (in Kb)")
options.register ('input',
          '',
          VarParsing.multiplicity.singleton,
          VarParsing.varType.string,
          "search eos area for files")
options.parseArguments()

print "xrdfs root://cmseos.fnal.gov ls " + options.input
files = (subprocess.check_output("xrdfs root://cmseos.fnal.gov ls " + options.input, shell=True)).split()
print files

process = cms.Process("Merge")
process.source = cms.Source ("PoolSource",
    fileNames = cms.untracked.vstring (files),
    duplicateCheckMode = cms.untracked.string('noDuplicateCheck')
)

if options.eventsToProcess:
    process.source.eventsToProcess = \
           cms.untracked.VEventRange (options.eventsToProcess)


process.maxEvents = cms.untracked.PSet(
      input = cms.untracked.int32 (options.maxEvents)
)


process.Out = cms.OutputModule("PoolOutputModule",
        fileName = cms.untracked.string (options.outputFile)
)

if options.maxSize:
    process.Out.maxSize = cms.untracked.int32 (options.maxSize)

process.end = cms.EndPath(process.Out)
