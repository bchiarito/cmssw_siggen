# Auto generated configuration file
# using: 
# Revision: 1.19 
# Source: /local/reps/CMSSW/CMSSW/Configuration/Applications/python/ConfigBuilder.py,v 
# with command line options: Configuration/GenProduction/python/Hadronizer_TuneCUETP8M1_13TeV_Phi2simpttqed_54_eta_LHE_pythia8_cff.py --fileout file:GEN.root --filein file:../../events.lhe --mc --eventcontent RAWSIM --datatier GEN --conditions 106X_upgrade2018_realistic_v15_L1v1 --beamspot Realistic25ns13TeVEarly2018Collision --step GEN --geometry DB:Extended --era Run2_2018 --python_filename GEN_2018_cfg.py -n -1 --no_exec
import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
options = VarParsing ("python")
options.register("inputFile", "", VarParsing.multiplicity.singleton, VarParsing.varType.string, "")
options.register("hadronizer", "", VarParsing.multiplicity.singleton, VarParsing.varType.string, "")
options.register("numEvents", -1, VarParsing.multiplicity.singleton, VarParsing.varType.int, "")
options.setDefault("outputFile", "file:GEN.root")
options.parseArguments()
from Configuration.Eras.Era_Run2_2018_cff import Run2_2018

process = cms.Process('GEN',Run2_2018)

# import of standard configurations
process.load('Configuration.StandardSequences.Services_cff')
process.load('SimGeneral.HepPDTESSource.pythiapdt_cfi')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.load('Configuration.EventContent.EventContent_cff')
process.load('SimGeneral.MixingModule.mixNoPU_cfi')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Generator_cff')
process.load('IOMC.EventVertexGenerators.VtxSmearedRealistic25ns13TeVEarly2018Collision_cfi')
process.load('GeneratorInterface.Core.genFilterSummary_cff')
process.load('Configuration.StandardSequences.EndOfProcess_cff')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

process.maxEvents = cms.untracked.PSet(
        input = cms.untracked.int32(options.numEvents)
)

# Input source
process.source = cms.Source("LHESource",
                            fileNames = cms.untracked.vstring(options.inputFile)
)

process.options = cms.untracked.PSet(

)

# Production Info
process.configurationMetadata = cms.untracked.PSet(
    annotation = cms.untracked.string(''),
    name = cms.untracked.string('Applications'),
    version = cms.untracked.string('$Revision: 1.19 $')
)

# Output definition

process.RAWSIMoutput = cms.OutputModule("PoolOutputModule",
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('generation_step')
    ),
    compressionAlgorithm = cms.untracked.string('LZMA'),
    compressionLevel = cms.untracked.int32(1),
    dataset = cms.untracked.PSet(
        dataTier = cms.untracked.string('GEN'),
        filterName = cms.untracked.string('')
    ),
    eventAutoFlushCompressedSize = cms.untracked.int32(20971520),
    fileName = cms.untracked.string(options.outputFile),
    outputCommands = process.RAWSIMEventContent.outputCommands,
    splitLevel = cms.untracked.int32(0)
)

# Additional output definition

# Other statements
process.genstepfilter.triggerConditions=cms.vstring("generation_step")
from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, '106X_upgrade2018_realistic_v15_L1v1', '')

#54 (for steffie's signal)
if options.hadronizer =="54":
        process.generator = cms.EDFilter("Pythia8HadronizerFilter",
            PythiaParameters = cms.PSet(
                parameterSets = cms.vstring(
                    'pythia8CommonSettings', 
                    'pythia8CUEP8M1Settings', 
                    'processParameters'
                ),
                processParameters = cms.vstring(
                    '54:new', 
                    '54:name = pomega_bsm', 
                    '54:spinType = 1', 
                    '54:chargeType = 0', 
                    '54:colType = 0', 
                    '54:mayDecay = true', 
                    '54:addChannel = 1 0.3931181 0 22 22', 
                    '54:addChannel = 1 0.3257150 0 111 111 111', 
                    '54:addChannel = 1 0.0002700 0 111 22 22', 
                    '54:addChannel = 1 0.2274105 0 211 -211 111', 
                    '54:addChannel = 1 0.0460021 0 211 -211 22', 
                    '54:addChannel = 1 0.0069003 11 22 11 -11', 
                    '54:addChannel = 1 0.0003100 11 22 13 -13', 
                    '54:addChannel = 1 0.0000060 0 13 -13', 
                    '54:addChannel = 1 0.0002680 12 211 -211 11 -11'
                ),
                pythia8CUEP8M1Settings = cms.vstring(
                    'Tune:pp 14', 
                    'Tune:ee 7', 
                    'MultipartonInteractions:pT0Ref=2.4024', 
                    'MultipartonInteractions:ecmPow=0.25208', 
                    'MultipartonInteractions:expPow=1.6'
                ),
                pythia8CommonSettings = cms.vstring(
                    'Tune:preferLHAPDF = 2', 
                    'Main:timesAllowErrors = 10000', 
                    'Check:epTolErr = 0.01', 
                    'Beams:setProductionScalesFromLHEF = off', 
                    'SLHA:keepSM = on', 
                    'SLHA:minMassSM = 1000.', 
                    'ParticleDecays:limitTau0 = on', 
                    'ParticleDecays:tau0Max = 10', 
                    'ParticleDecays:allowPhotonRadiation = on'
                )
            ),
            comEnergy = cms.double(13000.0),
            filterEfficiency = cms.untracked.double(1.0),
            maxEventsToPrint = cms.untracked.int32(1),
            pythiaHepMCVerbosity = cms.untracked.bool(False),
            pythiaPylistVerbosity = cms.untracked.int32(1)
        )

#54_twoprongdecay (for steffie's signal)
elif options.hadronizer =="54_twoprongdecay":
        process.generator = cms.EDFilter("Pythia8HadronizerFilter",
            PythiaParameters = cms.PSet(
                parameterSets = cms.vstring(
                    'pythia8CommonSettings', 
                    'pythia8CUEP8M1Settings', 
                    'processParameters'
                ),
                processParameters = cms.vstring(
                    '54:new', 
                    '54:name = pomega_bsm', 
                    '54:spinType = 1', 
                    '54:chargeType = 0', 
                    '54:colType = 0', 
                    '54:mayDecay = true', 
#                    '54:addChannel = 1 0.3931181 0 22 22', 
#                    '54:addChannel = 1 0.3257150 0 111 111 111', 
#                    '54:addChannel = 1 0.0002700 0 111 22 22', 
                    '54:addChannel = 1 0.2274105 0 211 -211 111', 
                    '54:addChannel = 1 0.0460021 0 211 -211 22', 
#                    '54:addChannel = 1 0.0069003 11 22 11 -11', 
#                    '54:addChannel = 1 0.0003100 11 22 13 -13', 
#                    '54:addChannel = 1 0.0000060 0 13 -13', 
#                    '54:addChannel = 1 0.0002680 12 211 -211 11 -11'
                ),
                pythia8CUEP8M1Settings = cms.vstring(
                    'Tune:pp 14', 
                    'Tune:ee 7', 
                    'MultipartonInteractions:pT0Ref=2.4024', 
                    'MultipartonInteractions:ecmPow=0.25208', 
                    'MultipartonInteractions:expPow=1.6'
                ),
                pythia8CommonSettings = cms.vstring(
                    'Tune:preferLHAPDF = 2', 
                    'Main:timesAllowErrors = 10000', 
                    'Check:epTolErr = 0.01', 
                    'Beams:setProductionScalesFromLHEF = off', 
                    'SLHA:keepSM = on', 
                    'SLHA:minMassSM = 1000.', 
                    'ParticleDecays:limitTau0 = on', 
                    'ParticleDecays:tau0Max = 10', 
                    'ParticleDecays:allowPhotonRadiation = on'
                )
            ),
            comEnergy = cms.double(13000.0),
            filterEfficiency = cms.untracked.double(1.0),
            maxEventsToPrint = cms.untracked.int32(1),
            pythiaHepMCVerbosity = cms.untracked.bool(False),
            pythiaPylistVerbosity = cms.untracked.int32(1)
        )

#90000054 (brandon's signal)
elif options.hadronizer =="90000054":
        process.generator = cms.EDFilter("Pythia8HadronizerFilter",
            PythiaParameters = cms.PSet(
                parameterSets = cms.vstring(
                    'pythia8CommonSettings', 
                    'pythia8CUEP8M1Settings', 
                    'processParameters'
                ),
                processParameters = cms.vstring(
                    '90000054:new',
                    '90000054:name = pomega_bsm',
                    '90000054:spinType = 1',
                    '90000054:chargeType = 0',
                    '90000054:colType = 0',
                    '90000054:mayDecay = true',
                    # from pythia eta entry: m0="0.54785"
                    '90000054:addChannel = 1 0.3931181 0 22 22',
                    '90000054:addChannel = 1 0.3257150 0 111 111 111',
                    '90000054:addChannel = 1 0.0002700 0 111 22 22',
                    '90000054:addChannel = 1 0.2274105 0 211 -211 111',
                    '90000054:addChannel = 1 0.0460021 0 211 -211 22',
                    '90000054:addChannel = 1 0.0069003 11 22 11 -11',
                    '90000054:addChannel = 1 0.0003100 11 22 13 -13',
                    '90000054:addChannel = 1 0.0000060 0 13 -13',
                    '90000054:addChannel = 1 0.0002680 12 211 -211 11 -11'
                    ),
                pythia8CUEP8M1Settings = cms.vstring(
                    'Tune:pp 14', 
                    'Tune:ee 7', 
                    'MultipartonInteractions:pT0Ref=2.4024', 
                    'MultipartonInteractions:ecmPow=0.25208', 
                    'MultipartonInteractions:expPow=1.6'
                ),
                pythia8CommonSettings = cms.vstring(
                    'Tune:preferLHAPDF = 2', 
                    'Main:timesAllowErrors = 10000', 
                    'Check:epTolErr = 0.01', 
                    'Beams:setProductionScalesFromLHEF = off', 
                    'SLHA:keepSM = on', 
                    'SLHA:minMassSM = 1000.', 
                    'ParticleDecays:limitTau0 = on', 
                    'ParticleDecays:tau0Max = 10', 
                    'ParticleDecays:allowPhotonRadiation = on'
                )
            ),
            comEnergy = cms.double(13000.0),
            filterEfficiency = cms.untracked.double(1.0),
            maxEventsToPrint = cms.untracked.int32(1),
            pythiaHepMCVerbosity = cms.untracked.bool(False),
            pythiaPylistVerbosity = cms.untracked.int32(1)
        )

else: 
        print "ERROR: No valid hadronizer chosen. Exiting..."
        print options.hadronizer
        exit()

process.ProductionFilterSequence = cms.Sequence(process.generator)

# Path and EndPath definitions
process.generation_step = cms.Path(process.pgen)
process.genfiltersummary_step = cms.EndPath(process.genFilterSummary)
process.endjob_step = cms.EndPath(process.endOfProcess)
process.RAWSIMoutput_step = cms.EndPath(process.RAWSIMoutput)

# Schedule definition
process.schedule = cms.Schedule(process.generation_step,process.genfiltersummary_step,process.endjob_step,process.RAWSIMoutput_step)
from PhysicsTools.PatAlgos.tools.helpers import associatePatAlgosToolsTask
associatePatAlgosToolsTask(process)
# filter all path with the production filter sequence
for path in process.paths:
	getattr(process,path).insert(0, process.ProductionFilterSequence)


# Customisation from command line

# Add early deletion of temporary data products to reduce peak memory need
from Configuration.StandardSequences.earlyDeleteSettings_cff import customiseEarlyDelete
process = customiseEarlyDelete(process)
# End adding early deletion
