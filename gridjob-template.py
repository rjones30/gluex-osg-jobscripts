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
import subprocess

container = "/cvmfs/singularity.opensciencegrid.org/rjones30/gluex:latest"
templates = "/cvmfs/oasis.opensciencegrid.org/gluex/templates"
calib_db = "/cvmfs/oasis.opensciencegrid.org/gluex/ccdb/1.06.03/sql/ccdb_2017-06-09.sqlite"
resources = "/cvmfs/oasis.opensciencegrid.org/gluex/resources"
jobname = re.sub(r"\.py$", "", os.path.basename(__file__))

# define the run range and event statistics here

total_events_to_generate = 10000       # aggregate for all slices in this job
number_of_events_per_slice = 250       # how many events generated per job
number_of_slices_per_run = 50          # increment run number after this many slices
initial_run_number = 31001             # starting value for generated run number

### This is the start of the section that users normally need to customize ###

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
   calib_db_copy = "/tmp/" + os.path.basename(calib_db) + "-" + str(os.getpid())
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
   simoutput = "hdgeant" + suffix + ".hddm"
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
   fort15.close()
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
         controlin.write("OUTFILE 'hdgeant.hddm'\n")
      elif re.match(r"^[Cc]*TRIG", line):
         controlin.write("TRIG " + str(number_of_events_per_slice) + "\n")
      elif re.match(r"^[Cc]*SWIT", line):
         controlin.write("SWIT 0 0 0 0 0 0 0 0 0 0\n")
      elif re.match(r"^[Cc]*RUNG", line):
         controlin.write("cRUNG " + str(run_number) + "\n")
      elif re.match(r"^[Cc]*DEBU", line):
         controlin.write("cDEBU 1 10 1000\n")
      else:
         controlin.write(line)
   controlin.close()
   retcode = shellcode("export JANA_CALIB_CONTEXT=variation=mc",
                       "export JANA_CALIB_URL=sqlite:///" + calib_db,
                       "export JANA_RESOURCE_DIR=" + resources,
                       "hdgeant4")
   if retcode == 0:
      os.rename("hdgeant.hddm", output_hddmfile)
   else:
      os.remove("hdgeant.hddm")
   os.remove("geant.hbook")
   os.remove("hdgeant.rz")
   os.remove("control.in")
   sys.exit(0)
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
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=1800 " +
                       "        -PTHREAD_TIMEOUT=3600 " +
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
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=1800 " +
                       "        -PTHREAD_TIMEOUT=3600 " +
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
                       "        -PTHREAD_TIMEOUT_FIRST_EVENT=1800 " +
                       "        -PTHREAD_TIMEOUT=3600 " +
                       "        -PPLUGINS=monitoring_hists " +
                       input_hddmfile)
   if retcode == 0:
      os.rename("hd_root.root", output_rootfile)
   else:
      os.remove("hd_root.root")
   return retcode

### This is the end of the section that users normally need to customize ###

def usage():
   """
   Print a usage message and exit
   """
   Print("Usage:", jobname, "command [command arguments]")
   Print(" where command is one of:")
   Print("   info - prints a brief description of this job")
   Print("   status - reports the execution status of the job")
   Print("   submit - submits the job for running on the osg")
   Print("   cancel - cancels the job if it is running on the osg")
   Print("   doslice - executes the job in the present context")
   Print("")
   Print(" info command arguments:")
   Print("   (none)")
   Print(" status command arguments:")
   Print("   -l - optional argument, generates a more detailed listing")
   Print(" submit command arguments:")
   Print("   <start> - first slice to submit to the grid, default 0")
   Print("   <count> - number of slices to submit, starting with <start>")
   Print("   If <count> is 0 or missing then its value is computed to meet")
   Print("   the goal of \"total_events_to_generate\" specified in the")
   Print("   header of this job script.")
   Print(" cancel command arguments:")
   Print("   <start> - first slice to cancel on the grid, default 0")
   Print("   <count> - number of slices to cancel, starting with <start>")
   Print("   If <count> is 0 or missing then its value is computed to meet")
   Print("   the goal of \"total_events_to_generate\" specified in the")
   Print("   header of this job script.")
   Print(" doslice command arguments:")
   Print("   <start> - base slice number for this job, see info for valid range")
   Print("   <offset> - the slice number to generate is <start> + <offset>")
   Print("   Only one slice can be generated per invocation of this script.")
   Print("   Both arguments are mandatory.")
   Print("")
   sys.exit(0)

def do_info(arglist):
   """
   Prints a compact summary of information about the job.
   """
   lines = 0
   for line in open(__file__):
      lines += 1
      if lines == 1:
         continue
      if re.match(r"^#", line) and lines < 100:
         Print(line.lstrip("#").rstrip())
      else:
         Print("")
         break
  

def do_status(arglist):
   """
   Prints a report on the execution status of the job.
   There is one optional argument, "-l" for a detailed listing.
   """
   logdir = jobname + ".logs"
   logfile = logdir + "/" + jobname + ".log"
   batchfile = logdir + "/" + "batches.log"
   if not os.path.exists(logfile) or not os.path.exists(batchfile):
      Print("No record exists of this job ever having been submitted.")
      sys.exit(0)
   longlisting = 0
   if len(arglist) > 0:
      if len(arglist) == 1 and arglist[0] == "-l":
         longlisting = 1
      else:
         usage()
   batch_start = {}
   batch_count = {}
   min_start = 0
   max_count = 0
   for line in open(batchfile):
      fields = line.rstrip().split()
      batch = int(fields[0])
      batch_start[batch] = int(fields[1])
      if batch_start[batch] < min_start:
         min_start = batch_start[batch]
      batch_count[batch] = int(fields[2])
      if batch_count[batch] > max_count:
         max_count = batch_count[batch]
   state = {}
   cout = shellpipe("condor_userlog " + logfile)
   for line in cout:
      line = line.rstrip()
      m = re.match(r"^([0-9]+)\.([0-9]+)(.*)", line)
      if m:
         batch = int(m.group(1))
         if not batch in batch_start:
            Print("Error - cluster", batch, "in the condor log was not",
                  "recorded in the batches.log file, skipping this line!")
            continue
         offset = int(m.group(2))
         sliceno = batch_start[batch] + offset
         line = ("slice " + str(sliceno)).ljust(15)
         fields = line.split()
         evict = fields[3]
         wall = fields[4]
         good = fields[5]
         if wall == "0+00:00":
            state[sliceno] = "submitted"
         elif good != "0+00:00":
            state[sliceno] = "completed"
         elif evict == "0+00:00":
            state[sliceno] = "running"
         else:
            state[sliceno] = "evicted"
      if longlisting:
         Print(line)
   submitted = 0
   completed = 0
   running = 0
   failed = 0
   total = 0
   for sliceno in state:
      total += 1
      if state[sliceno] == "submitted":
         submitted += 1
      elif state[sliceno] == "completed":
         completed += 1
      elif state[sliceno] == "running":
         running += 1
      elif state[sliceno] == "failed":
         failed += 1
   Print("Total statistics for job", jobname, ":")
   Print("  slices submitted: ", submitted)
   Print("  slices completed: ", completed)
   Print("  slices running: ", running)
   Print("  slices failed: ", failed)
   Print("For more details, do condor_userlog", logfile)

def do_submit(arglist):
   """
   Submit this job for execution on the osg, assuming it is not already running.
   Arguments are:
     1) <start> - first slice number to be executed in this submission
     2) <count> - number of slices to be executed, starting with <start>
   Both arguments are optional, defaulting to 0 and full slice count.
   """
   try:
      if len(arglist) > 0:
         start = int(arglist[0])
      else:
         start = 0
      if len(arglist) > 1:
         count = int(arglist[1])
      else:
         count = 0
   except:
      usage()
   if count == 0:
      nchunk = number_of_events_per_slice
      nslices = int((total_events_to_generate + nchunk - 1) / nchunk)
      count = nslices - count
   if start < 0:
      Print("Error - start slice is less than zero!")
      Print("Nothing to do, quitting...")
      sys.exit(1)
   if count <= 0:
      Print("Error - start slice is greater than the job max slice count!",
            "Nothing to do, quitting...")
      sys.exit(1)
   logdir = jobname + ".logs"
   logfile = logdir + "/" + jobname + ".log"
   batchfile = logdir + "/" + "batches.log"
   batch = 0
   if os.path.exists(batchfile):
      for line in open(batchfile):
         if len(line) > 0:
            batch += 1
   submitfile = logdir + "/" + jobname + ".sub" + str(batch)
   if not os.path.isdir(logdir):
      os.makedirs(logdir)
   submit_out = open(submitfile, "w")
   for line in open(templates + "/osg-condor.sub"):
      line = line.rstrip()
      if re.match(r"^Requirements = ", line):
         submit_out.write("Requirements = (HAS_SINGULARITY == TRUE)")
         submit_out.write(" && (HAS_CVMFS_oasis_opensciencegrid_org == True)")
         submit_out.write("\n")
      elif re.match(r"^\+SingularityImage = ", line):
         submit_out.write("+SingularityImage = \"" + container + "\"\n")
         submit_out.write("+SingularityBindCVMFS = True\n")
         submit_out.write("+SingularityAutoLoad = True\n")
      elif re.match(r"\+SingularityBindCVMFS = ", line):
         pass
      elif re.match(r"\+SingularityAutoLoad = ", line):
         pass
      elif re.match(r"^transfer_input_files = ", line):
         submit_out.write("transfer_input_files = " + jobname + ".py\n")
      elif re.match(r"^x509userproxy = ", line):
         if shellcode("voms-proxy-info -exists -valid 24:00") != 0:
            Print("Error - your grid certificate must be valid, and have",
                  "at least 24 hours left in order for you to submit this job.")
            sys.exit(1)
         proxy = backticks("voms-proxy-info -path")
         submit_out.write("x509userproxy = " + proxy + "\n")
      elif re.match(r"^initialdir = ", line):
         submit_out.write("initialdir = " + os.getcwd() + "\n")
      elif re.match(r"^output = ", line):
         submit_out.write("output = " + logdir + "/$(CLUSTER).$(PROCESS).out\n")
      elif re.match(r"^error = ", line):
         submit_out.write("error = " + logdir + "/$(CLUSTER).$(PROCESS).err\n")
      elif re.match(r"^log = ", line):
         submit_out.write("log = " + logdir + "/" + jobname + ".log\n")
      elif re.match(r"^executable =", line):
         submit_out.write("executable = osg-container.sh\n")
      elif re.match(r"^arguments = ", line):
         submit_out.write("arguments = ./" + jobname + ".py" +
                          " doslice " + str(start) + " $(PROCESS)\n")
      elif re.match(r"^queue", line):
         submit_out.write("queue " + str(count) + "\n")
      else:
         submit_out.write(line + "\n")
   # todo: actually submit the job to condor
   cluster = 9876 + batch
   if os.path.exists(batchfile):
      batches_out = open(batchfile, "a")
   else:
      batches_out = open(batchfile, "w")
   batches_out.write(str(cluster) + " " + str(start) + " " + str(count) + "\n")

def do_cancel(arglist):
   """
   Cancel execution of this job on the osg, assuming it is already running.
   Arguments are:
     1) <start> - first slice number to be cancelled by this request
     2) <count> - number of slices to be cancelled, starting with <start>
   Both arguments are optional, defaulting to 0 and full slice count.
   """
   try:
      if len(arglist) > 0:
         start = int(arglist[0])
      else:
         start = 0
      if len(arglist) > 1:
         count = int(arglist[1])
      else:
         count = 0
   except:
      usage()
   if count == 0:
      nchunk = number_of_events_per_slice
      nslices = int((total_events_to_generate + nchunk - 1) / nchunk)
      count = nslices - count
   if start < 0:
      Print("Error - start slice is less than zero!",
            "Nothing to do, quitting...")
      sys.exit(1)
   if count <= 0:
      Print("Error - start slice is greater than the job max slice count!",
            "Nothing to do, quitting...")
      sys.exit(1)
   logdir = jobname + ".logs"
   batchfile = logdir + "/" + "batches.log"
   if not os.path.exists(batchfile):
      Print("Error - no record exists of this job ever having been submitted!")
      sys.exit(1)
   cancel_range = {}
   for line in open(batchfile):
      fields = line.rstrip().split()
      batch = int(fields[0])
      first = int(fields[1])
      number = int(fields[2])
      lim0 = start - first
      lim1 = lim0 + count
      if lim0 < 0:
         lim0 = 0
      if lim1 > number:
         lim1 = number
      cancel_range[batch] = (lim0, lim1)
      if lim1 > lim0:
         Print("todo: condor_rm", str(batch) + "." + str(lim0) + "-" + str(lim1))

def validate_customizations():
   """
   Check that this script has been properly customized by the user,
   otherwise refuse to continue.
   """
   lines = 0
   for line in open(__file__):
      if re.search(r"TEMPLATE JOB SCRIPT FOR GLUEX OSG PRODUCTION", line) or\
         re.search(r"YOU MUST REPLACE .* ABOVE WITH A UNIQUE NAME", line) or\
         re.search(r"CUSTOMIZE THE SCRIPT SO THE JOB DOES WHAT YO", line) or\
         re.search(r"HEADER LINES (THE ONES BETWEEN THE ===) WITH", line) or\
         re.search(r"WHAT IT DOES, AND SAVE IT UNDER THE NEW NAME", line) or\
         re.search(r"^# jobname: gridjob-template", line) or\
         re.search(r"^# author: gluex.experimenter@jlab.org", line) or\
         re.search(r"# created: jan 1, 1969",line):
         return 1
      lines += 1
      if lines > 30:
         break
   return 0

def backticks(*args):
   """
   Emulates the `shell_command` behavior of the standard unix shells.
   The shell exit code is available as global variable backticks_errcode.
   """
   request = subprocess.Popen(";".join(args), shell=True,
                              stdout=subprocess.PIPE)
   global backticks_errcode
   backticks_errcode = request.wait()
   return request.communicate()[0].rstrip()

def shellpipe(*args):
   """
   Emulates the `shell_command` behavior of the standard unix shells,
   but returns a pipe from which the output can be read instead of
   capturing it all in memory first. Use this as a replacement for
   backticks if the size of the output might be too large to fit in
   memory at one time.
   """
   p = subprocess.Popen(";".join(args), shell=True, stdout=subprocess.PIPE)
   return p.communicate()[0]

def shellcode(*args):
   """
   Executes a sequence of shell commands, and returns the exit code
   from the last one in the sequence. Output is not captured.
   """
   return subprocess.call(";".join(args), shell=True)

def Print(*args):
   """
   Custom print statement for this script, to provide immediate
   flushing of stdout after each write, and to simplify eventual
   migration to python 3, where print "msg" no longer works.
   """
   sys.stdout.write(" ".join([str(arg) for arg in args]))
   sys.stdout.write("\n")
   sys.stdout.flush()

# check script and arguments for validity

if len(sys.argv) < 2 or re.match(r"^-", sys.argv[1]):
   usage()
   sys.exit(1)

if validate_customizations() != 0:
   Print("Error - this job script is a template;",
         "it must be customized before you can run it.")
   sys.exit(1)

if sys.argv[1] == "info":
   do_info(sys.argv[2:])
elif sys.argv[1] == "status":
   do_status(sys.argv[2:])
elif sys.argv[1] == "submit":
   do_submit(sys.argv[2:])
elif sys.argv[1] == "cancel":
   do_cancel(sys.argv[2:])
elif sys.argv[1] == "doslice":
   do_slice(sys.argv[2:])
else:
   usage()
