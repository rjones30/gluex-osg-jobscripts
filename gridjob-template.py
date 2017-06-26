#!/usr/bin/env python2.7
#
# jobname: gridjob-template
# author: gluex.experimenter@jlab.org
# created: jan 1, 1969
#
#======================================================================
# TEMPLATE JOB SCRIPT FOR GLUEX OSG PRODUCTION
#
# YOU MUST REPLACE "gridjob-template" ABOVE WITH A UNIQUE NAME FOR THIS 
# JOB, CUSTOMIZE THE SCRIPT SO THE JOB DOES WHAT YOU WANT, REPLACE THESE
# HEADER LINES (THE ONES BETWEEN THE ===) WITH A BRIEF DESCRIPTION OF
# WHAT IT DOES, AND SAVE IT UNDER THE NEW NAME.
#======================================================================


import sys
import os
import re
import tempfile
import subprocess

python_mods = "/cvmfs/singularity.opensciencegrid.org/rjones30/gluex:latest/usr/lib/python2.7/site-packages"
calib_db = "/cvmfs/oasis.opensciencegrid.org/gluex/ccdb/1.06.03/sql/ccdb_2017-06-09.sqlite"
resources = "/cvmfs/oasis.opensciencegrid.org/gluex/resources"
templates = "/cvmfs/oasis.opensciencegrid.org/gluex/templates"
jobname = re.sub(r"\.py$", "", os.path.basename(__file__))

# define the run range and event statistics here

total_events_to_generate = 10000       # aggregate for all slices in this job
number_of_events_per_slice = 250       # how many events generated per job
number_of_slices_per_run = 50          # increment run number after this many slices
initial_run_number = 31001             # starting value for generated run number

try:
   sys.path.append(python_mods)
   from osg_job_helper import *
except:
   print "Error - this job script expects to find the cernvm filesystem mounted at /cvmfs"
   print "with /cvmfs/singularity.opensciencegrid.org and /cvmfs/oasis.opensciencegrid.org"
   print "reachable by automount."
   sys.exit(1)
helper_set_slicing(total_events_to_generate, number_of_events_per_slice)

### All processing occurs in these functions -- users should customize these as needed ###

def do_slice(arglist):
   """
   Actually execute this slice here and now. Normally this action takes place
   on the worker node out on the osg, but it can also be executed from an
   interactive shell. The results should be the same in either case.
   Arguments are:
     1) <start> - base slice number to be executed by this request
     2) <offset> - slice number (only 1) to execute is <start> + <offset>
   """
   if len(arglist) != 2:
      usage()

   global run_number
   global slice_index
   slice_index = int(arglist[0]) + int(arglist[1])
   run_number = initial_run_number + int(slice_index / number_of_slices_per_run)
   suffix = "_" + jobname + "_" + str(slice_index)

   # make a copy of ccdb sqlite file in /tmp to be sure file locking works
   global calib_db
   calib_db_copy = tempfile.NamedTemporaryFile().name
   if os.path.exists(calib_db_copy):
      Print("Warning - ccdb sqlite file", calib_db_copy,
            "already exists in tmp directory, assuming it is ok!")
   elif shellcode("cp -f " + calib_db + " " + calib_db_copy) != 0:
      Print("Error - unable to make a local copy of", calib_db_copy,
            "so cannot start job in this container!")
      sys.exit(77)
   calib_db = calib_db_copy
   
   # step 1: MC generation
   mcoutput = "bggen" + suffix + ".hddm"
   if os.path.exists(mcoutput):
      Print("MonteCarlo generator output file", mcoutput,
            "already exists, keeping.")
      sys.stdout.flush()
   else:
      err = do_mcgeneration(mcoutput)
      if err != 0:
         return err

   # step 2: Physics simulation
   simoutput = "hdgeant4" + suffix + ".hddm"
   if os.path.exists(simoutput) and \
      os.path.getmtime(simoutput) > os.path.getmtime(mcoutput):
      Print("Physics simulation output file", simoutput,
            "already exists, keeping.")
      sys.stdout.flush()
   else:
      err = do_mcsimulation(mcoutput, simoutput)
      if err != 0:
         return err
      else:
         os.remove(mcoutput)

   # step 3: MC smearing
   smearoutput = "mcsmear" + suffix + ".hddm"
   if os.path.exists(smearoutput) and \
      os.path.getmtime(smearoutput) > os.path.getmtime(simoutput):
      Print("MC smearing output file", smearoutput,
            "already exists, keeping.")
   else:
      err = do_mcsmearing(simoutput, smearoutput)
      if err != 0:
         return err
      else:
         os.remove(simoutput)

   # step 4: Event reconstruction
   restoutput = "rest" + suffix + ".hddm"
   if os.path.exists(restoutput) and \
      os.path.getmtime(restoutput) > os.path.getmtime(smearoutput):
      Print("Rest reconstruction output file", restoutput,
            "already exists, keeping.")
   else:
      err = do_reconstruction(smearoutput, restoutput)
      if err != 0:
         return err
      else:
         os.remove(smearoutput)

   # step 5: Event analysis
   rootoutput = "ana" + suffix + ".root"
   if os.path.exists(rootoutput) and \
      os.path.getmtime(rootoutput) > os.path.getmtime(restoutput):
      Print("Root analysis output file", rootoutput,
            "already exists, keeping.")
   else:
      err = do_analysis(restoutput, rootoutput)
      if err != 0:
         return err

   # return success!
   os.remove(calib_db_copy)
   return 0

def do_mcgeneration(output_hddmfile):
   """
   Take the actions to generate a slice of Monte Carlo events for this
   job. You should customize this function for your needs. The return
   value should be 0 for success, non-zero if the action could not be
   completed due to errors. If this step is not needed, simply return 0.
   """
   randomseed = slice_index + 12345
   fort15 = open("fort.15", "w")
   for line in open(templates + "/run.ffr.template"):
      if re.match(r"^TRIG", line):
         fort15.write("TRIG" + str(number_of_events_per_slice).rjust(11))
         fort15.write("         number of events to simulate\n")
      elif re.match(r"^WROUT", line):
         fort15.write("WROUT    1    0    0    hddm output only\n")
      elif re.match(r"^RUNNO", line):
         fort15.write("RUNNO" + str(run_number).rjust(10))
         fort15.write("         run number for output events\n")
      elif re.match(r"^NPRIEV", line):
         fort15.write("NPRIEV    5             only print first few events\n")
      elif re.match(r"^EPHLIM", line):
         fort15.write("EPHLIM    8.4    9.0    energy range in GeV\n")
      elif re.match(r"^RNDMSEQ", line):
         fort15.write("RNDMSEQ" + str(randomseed).rjust(8))
         fort15.write("         random number sequence\n")
      elif re.match(r"^ZCOLLIM", line):
         fort15.write("DCOLLIM" + "0.005".rjust(8))
         fort15.write("         collimator diameter in m\n")
         fort15.write(line)
      else:
         fort15.write(line)
   fort15 = 0
   inputfiles = ("pythia-geant.map", "pythia.dat", "particle.dat")
   for infile in inputfiles:
      err = shellcode("cp " + templates + "/" + infile + " " + infile)
      if err != 0:
         Print("Error - cannot copy bggen templates from cvmfs store,",
               "quitting!")
         sys.exit(1)
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "bggen")
   if retcode == 0:
      os.rename("bggen.hddm", output_hddmfile)
   else:
      os.remove("bggen.hddm")
   for infile in inputfiles:
      os.remove(infile)
   for temp in "bggen.his", "fort.15":
      os.remove(temp)
   return retcode

def do_mcsimulation(input_hddmfile, output_hddmfile):
   """
   Take the actions to simulate a slice of Monte Carlo events for this
   job. You should customize this function for your needs. The return
   value should be 0 for success, non-zero if the action could not be
   completed due to errors. If this step is not needed, simply return 0.
   """
   randomseed = slice_index + 123456789
   controlin = open("control.in", "w")
   for line in open(templates + "/control.in"):
      if re.match(r"^[Cc]*INFI", line):
         controlin.write("INFILE '" + input_hddmfile + "'\n")
      elif re.match(r"^[Cc]*OUTFI", line):
         controlin.write("OUTFILE 'hdgeant4.hddm'\n")
      elif re.match(r"^[Cc]*TRIG", line):
         controlin.write("TRIG " + str(number_of_events_per_slice) + "\n")
      elif re.match(r"^[Cc]*SWIT", line):
         controlin.write("SWIT 0 0 0 0 0 0 0 0 0 0\n")
      elif re.match(r"^[Cc]*RUNG", line):
         controlin.write("RUNG " + str(run_number) + "\n")
      elif re.match(r"^[Cc]*DEBU", line):
         controlin.write("cDEBU 1 10 1000\n")
      else:
         controlin.write(line)
   controlin = 0
   runmac = open("run.mac", "w")
   runmac.write("/run/beamOn " + str(number_of_events_per_slice) + "\n")
   runmac = 0
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "hdgeant4 run.mac")
   if retcode == 0:
      os.rename("hdgeant4.hddm", output_hddmfile)
   else:
      os.remove("hdgeant4.hddm")
   for temp in "control.in", "run.mac", "hdgeant.rz", "geant.hbook":
      if os.path.exists(temp):
         os.remove(temp)
   return retcode

def do_mcsmearing(input_hddmfile, output_hddmfile):
   """
   Take the actions to apply smearing to a slice of Monte Carlo events
   for this job. You should customize this function for your needs. The
   return value should be 0 for success, non-zero if the action could not be
   completed due to errors. If this step is not needed, simply return 0.
   """
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "mcsmear -PJANA:BATCH_MODE=1 " +
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=600 " +
                       "        -PTHREAD_TIMEOUT=600 " +
                       input_hddmfile)
   ofile = re.sub(r".hddm$", "_smeared.hddm", input_hddmfile)
   if retcode == 0:
      os.rename(ofile, output_hddmfile)
   else:
      os.remove(ofile)
   os.remove("smear.root")
   return retcode

def do_reconstruction(input_hddmfile, output_hddmfile):
   """
   Take the actions to reconstruct a slice of Monte Carlo events and
   generate REST output files. You should customize this function for your
   needs. The return value should be 0 for success, non-zero if the action
   could not be completed due to errors. If this step is not needed,
   simply return 0.
   """
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "hd_root -PJANA:BATCH_MODE=1 " +
                       "        -PNTHREADS=1 " +
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=600 " +
                       "        -PTHREAD_TIMEOUT=600 " +
                       "        -PPLUGINS=danarest,monitoring_hists " +
                       input_hddmfile)
   output_root = re.sub(r"\.hddm$", ".root", output_hddmfile)
   if retcode == 0:
      os.rename("dana_rest.hddm", output_hddmfile)
      os.rename("hd_root.root", output_root)
   else:
      os.remove("dana_rest.hddm")
      os.remove("hd_root.root")
   return retcode

def do_analysis(input_hddmfile, output_rootfile):
   """
   Take the actions to analyze a slice of Monte Carlo events and produce
   ROOT output files. You should customize this function for your needs.
   The return value should be 0 for success, non-zero if the action
   could not be completed due to errors. If this step is not needed,
   simply return 0.
   """
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "hd_root -PJANA:BATCH_MODE=1 " +
                       "        -PNTHREADS=1 " +
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=600 " +
                       "        -PTHREAD_TIMEOUT=600 " +
                       "        -PPLUGINS=monitoring_hists " +
                       input_hddmfile)
   if retcode == 0:
      os.rename("hd_root.root", output_rootfile)
   else:
      os.remove("hd_root.root")
   return retcode

### This is the end of the section that users normally need to customize ###

execute(sys.argv, do_slice)
