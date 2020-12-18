#!/usr/bin/env python3
#
# osgprod_bind.py - tool to find the scattered output files
#                   from Gluex production of raw data on the osg,
#                   and merge the outputs into the standard format
#                   used for archival on tape storage.
#
# author: richard.t.jones at uconn.edu
# version: december 8, 2020

import os
import sys
import time
import random
import struct
import psycopg2
import subprocess
import shutil
import re

dbserver = "cn445.storrs.hpc.uconn.edu"
dbname = "osgprod"
dbuser = "gluex"
dbpass = "slicing+dicing"

xrootd_url = "root://cn442.storrs.hpc.uconn.edu"
src_url = "srm://cn446.storrs.hpc.uconn.edu:8443"
dst_url = "gsiftp://scosg16.jlab.org/osgpool/halld/jonesrt/RunPeriod-2019-11"
dst_dn = "/C=US/O=Globus Consortium/OU=Globus Connect Service/CN=226c2da8-5236-11e9-a620-0a54e005f950"
dst_cred = os.path.expanduser("~") + "/.globus/proxy_scosg16"
input_area = "/gluex/resilient"
output_area = "/recon/ver01"

def db_connection():
   """
   Returns a persistent connection to the osgprod database.
   """
   global dbconnection
   try:
      conn = dbconnection
   except:
      dbconnection = psycopg2.connect(user = dbuser,
                                      password = dbpass,
                                      host = dbserver,
                                      port = "5432",
                                      database = dbname)
      conn = dbconnection
   return conn

def upload(outfile, outdir):
   """
   Uploads outfile to the storage element at dst_url under
   output directory outdir, returns 0 on success, raises an
   exception on error.
   """
   outpath = outdir + "/" + outfile
   my_env = os.environ.copy()
   my_env["X509_USER_PROXY"] = dst_cred
   for retry in range(0,99):
      try:
         subprocess.check_output(["globus-url-copy", "-create-dest",
                                  "-rst", "-stall-timeout", "300",
                                  "-ds", dst_dn, "-dst-cred", dst_cred,
                                  "file://" + os.getcwd() + "/" + outfile,
                                  dst_url + outpath], env=my_env)
         return 0
      except:
         continue
   subprocess.check_output(["globus-url-copy", "-create-dest",
                            "-rst", "-stall-timeout", "300",
                            "-ds", dst_dn, "-dst-cred", dst_cred,
                            "file://" + os.getcwd() + "/" + outfile,
                            dst_url + outpath], env=my_env)
   return 0

def next():
   """
   Gets the next output set to bind from the database, unpacks them into
   a temporary directory, and merges them at the input data file level
   into files in the output directory tree under the top-level dir vers.
   If the bindings db does not already exit, you must manually call
   create_table_bindings before trying to invoke this function.
   Return value is 1 if one rawdata file was processed successfully, 0
   if no data were found ready for binding, or < 0 if an error occurred.
   """
   iraw = 0
   run = 0
   seqno = 0
   with db_connection() as conn:
      with conn.cursor() as curs:
         try:
            curs.execute("""SELECT rawdata.id,rawdata.run,rawdata.seqno,
                                   slices.block1,slices.block2,
                                   jobs.cluster,jobs.process
                            FROM rawdata
                            LEFT JOIN bindings
                            ON bindings.iraw = rawdata.id
                            INNER JOIN slices
                            ON slices.iraw = rawdata.id
                            LEFT JOIN jobs
                            ON slices.ijob = jobs.id
                            AND jobs.exitcode = 0
                            WHERE bindings.id IS NULL
                            ORDER BY rawdata.id,slices.block1
                            LIMIT 2000;
                         """)
            slices_missing = 1
            for row in curs.fetchall():
               i = int(row[0])
               if i != iraw:
                  if slices_missing == 0:
                     break
                  else:
                     run = int(row[1])
                     seqno = int(row[2])
                     slices_missing = 0
                     slices = []
                     iraw = i
               block1 = int(row[3])
               block2 = int(row[4])
               if row[5] is not None and row[6] is not None:
                  cluster = int(row[5])
                  process = int(row[6])
                  slices.append((block1,block2,cluster,process))
               else:
                  print("slices missing on", row[5], row[6])
                  slices_missing += 1
            if slices_missing:
               return 0
            else:
               curs.execute("SELECT TIMEZONE('GMT', NOW());")
               now = curs.fetchone()[0]
               curs.execute("""INSERT INTO bindings
                               (iraw,starttime)
                               VALUES (%s,%s)
                               RETURNING id;
                            """, (iraw, now))
               row = curs.fetchone()
               if row:
                  ibind = int(row[0])
               else:
                  return 0
         except:
            iraw = 0
   if iraw == 0:
      time.sleep(random.randint(1,30))
      return -9 # collision

   workdir = str(iraw)
   os.mkdir(workdir)
   os.chdir(workdir)
   badslices = []
   for sl in slices:
      sdir = str(sl[0]) + "," + str(sl[1])
      os.mkdir(sdir)
      tarfile = "job_{0}_{1}.tar.gz".format(sl[2], sl[3])
      tarpath = input_area + "/" + tarfile
      try:
         subprocess.check_output(["gfal-copy", src_url + tarpath,
                                  "file://" + os.getcwd() + "/" + tarfile])
      except:
         sys.stderr.write("Error -999 on rawdata id {0}".format(iraw) +
                          " - job output " + tarfile + " is missing!\n")
         sys.stderr.flush()
         badslices.append(sdir)
         continue
      try:
         subprocess.check_output(["tar", "zxf", tarfile, "-C", sdir])
      except:
         sys.stderr.write("Error -999 on rawdata id {0}".format(iraw) +
                          " - job output " + tarfile + " is not readable!\n")
         sys.stderr.flush()
         badslices.append(sdir)
      finally:
         os.remove(tarfile)
   if len(badslices) > 0:
      with db_connection() as conn:
         with conn.cursor() as curs:
            curs.execute("SELECT TIMEZONE('GMT', NOW());")
            now = curs.fetchone()[0]
            curs.execute("""UPDATE bindings
                            SET endtime=%s, 
                                exitcode=%s,
                                details=%s
                            WHERE id = %s;
                         """, (now, -999, ":".join(badslices), ibind))
      os.chdir("..")
      shutil.rmtree(workdir)
      return 1

   badslices += merge_evio_skims(run, seqno, slices)
   badslices += merge_hddm_output(run, seqno, slices)
   badslices += merge_job_info(run, seqno, slices)
   badslices += merge_root_histos(run, seqno, slices)
   exitcode = -len(badslices)
   with db_connection() as comm:
      with conn.cursor() as curs:
         curs.execute("SELECT TIMEZONE('GMT', NOW());")
         now = curs.fetchone()[0]
         curs.execute("""UPDATE bindings
                         SET endtime=%s,
                             exitcode=%s,
                             details=%s
                         WHERE id = %s;
                      """, (now, exitcode, ":".join(badslices), ibind))
   os.chdir("..")
   shutil.rmtree(workdir)
   return 1

def merge_evio_skims(run, seqno, slices):
   """
   Merge the special events skim files from the individual jobs
   into a single skim file for each input raw data file. Returns
   0 on success, nonzero on error.
   """
   inset = {"BCAL-LED": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.BCAL-LED.evio",
            "DIRC-LED": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.DIRC-LED.evio",
            "FCAL-LED": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.FCAL-LED.evio",
            "CCAL-LED": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.CCAL-LED.evio",
            "random": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.random.evio",
            "omega": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.omega.evio",
            "sync": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.sync.evio",
            "ps": "hd_rawdata_{0:06d}_{1:03d}+{2},{3}.ps.evio",
           }
   outset = {"BCAL-LED": "BCAL-LED_{0:06d}_{1:03d}.evio",
             "DIRC-LED": "DIRC-LED_{0:06d}_{1:03d}.evio",
             "FCAL-LED": "FCAL-LED_{0:06d}_{1:03d}.evio",
             "CCAL-LED": "CCAL-LED_{0:06d}_{1:03d}.evio",
             "random": "random_{0:06d}_{1:03d}.evio",
             "omega": "omega_{0:06d}_{1:03d}.evio",
             "sync": "sync_{0:06d}_{1:03d}.evio",
             "ps": "ps_{0:06d}_{1:03d}.evio",
            }
   badslices = []
   slicepatt = re.compile(r"([1-9][0-9]*),([1-9][0-9]*)/")
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = []
      for sl in slices:
         ifile = "{0},{1}/".format(sl[0], sl[1]) +\
                 inset[iset].format(run, seqno, sl[0], sl[1])
         if iset == "sync" and not os.path.exists(ifile):
            print("Warning in merge_evio_skims - ",
                  "missing sync event skim ",
                  "in slice {0},{1}".format(sl[0], sl[1])
                 )
            continue
         elif iset == "omega" and not os.path.exists(ifile):
            print("Warning in merge_evio_skims - ",
                  "missing omega event skim ",
                  "in slice {0},{1}".format(sl[0], sl[1])
                 )
            continue
         ifiles.append(ifile)
      cmd = subprocess.Popen(["eviocat", "-o", ofile] + ifiles,
                             stderr=subprocess.PIPE)
      elog = cmd.communicate()
      if cmd.returncode != 0:
         for eline in elog[1].decode("ascii").split('\n'):
            badslice = slicepatt.search(eline)
            if badslice:
               badslices.append("{0},{1}".format(badslice.group(1),
                                                 badslice.group(2)))
            sys.stderr.write(eline + '\n')
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - evio file merging failed!\n")
         sys.stderr.flush()
         continue
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      upload(ofile, odir)
   return badslices

def merge_root_histos(run, seqno, slices):
   """
   Merge the output root files from the individual jobs into
   a single root file for each input raw data file. Returns
   0 on success, nonzero on error.
   """
   inset = {"hists": "hd_root.root",
            "tree_TS_scaler": "tree_TS_scaler.root",
            "tree_bcal_hadronic_eff": "tree_bcal_hadronic_eff.root",
            "tree_fcal_hadronic_eff": "tree_fcal_hadronic_eff.root",
            "tree_tof_eff": "tree_tof_eff.root",
            "tree_sc_eff": "tree_sc_eff.root",
            "tree_PSFlux": "tree_PSFlux.root",
            "tree_TPOL": "tree_TPOL.root",
           }
   outset = {"hists": "hd_root_{0:06d}_{1:03d}.root",
             "tree_TS_scaler": "tree_TS_scaler_{0:06d}_{1:03d}.root",
             "tree_bcal_hadronic_eff": "tree_bcal_hadronic_eff_{0:06d}_{1:03d}.root",
             "tree_fcal_hadronic_eff": "tree_fcal_hadronic_eff_{0:06d}_{1:03d}.root",
             "tree_tof_eff": "tree_tof_eff_{0:06d}_{1:03d}.root",
             "tree_sc_eff": "tree_sc_eff_{0:06d}_{1:03d}.root",
             "tree_PSFlux": "tree_PSFlux_{0:06d}_{1:03d}.root",
             "tree_TPOL": "tree_TPOL_{0:06d}_{1:03d}.root",
            }
   badslices = []
   slicepatt = re.compile(r"([1-9][0-9]*),([1-9][0-9]*)/")
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = ["{0},{1}/".format(sl[0], sl[1]) +
                inset[iset].format(run, seqno, sl[0], sl[1])
               for sl in slices
               ]
      cmd = subprocess.Popen(["hadd", ofile] + ifiles,
                             stderr=subprocess.PIPE)
      elog = cmd.communicate()
      if cmd.returncode != 0:
         for eline in elog[1].decode("ascii").split('\n'):
            badslice = slicepatt.search(eline)
            if badslice:
               badslices.append("{0},{1}".format(badslice.group(1),
                                                 badslice.group(2)))
            sys.stderr.write(eline + '\n')
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - root file merging failed!\n")
         sys.stderr.flush()
         continue
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      upload(ofile, odir)
   return badslices

def merge_hddm_output(run, seqno, slices):
   """
   Merge the output hddm files from the individual jobs into
   a single hddm file for each input raw data file. Returns
   0 on success, nonzero on error.
   """
   inset = {"REST": "dana_rest.hddm",
            "converted_random": "converted_random.hddm",
           }
   outset = {"REST": "dana_rest_{0:06d}_{1:03d}.hddm",
             "converted_random": "converted_random_{0:06d}_{1:03d}.hddm",
            }
   badslices = []
   slicepatt = re.compile(r"([1-9][0-9]*),([1-9][0-9]*)/")
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = []
      for sl in slices:
         ifile = "{0},{1}/".format(sl[0], sl[1]) +\
                 inset[iset].format(run, seqno, sl[0], sl[1])
         if iset == "converted_random" and not os.path.exists(ifile):
            print("Warning in merge_hddm_output - ",
                  "missing conveted_random output ",
                  "in slice {0},{1}".format(sl[0], sl[1])
                 )
            continue
         ifiles.append(ifile)
      cmd = subprocess.Popen(["hddmcat", "-o", ofile] + ifiles,
                             stderr=subprocess.PIPE)
      elog = cmd.communicate()
      if cmd.returncode != 0:
         for eline in elog[1].decode("ascii").split('\n'):
            badslice = slicepatt.search(eline)
            if badslice:
               badslices.append("{0},{1}".format(badslice.group(1),
                                                 badslice.group(2)))
            sys.stderr.write(eline + '\n')
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - hddm file merging failed!\n")
         sys.stderr.flush()
         continue
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      upload(ofile, odir)
   return badslices

def merge_job_info(run, seqno, slices):
   """
   Merge the job log hddm files from the individual jobs into
   a catenated log file for each input raw data file. Returns
   0 on success, nonzero on error.
   """
   inset = {"job_info": ["workscript.stdout", "workscript.stderr"],
           }
   outset = {"job_info": ["std_{0:06d}_{1:03d}.out", "std_{0:06d}_{1:03d}.err"],
            }
   tarset = {"job_info": "job_info_{0:06d}_{1:03d}.tgz",
            }
   badslices = []
   slicepatt = re.compile(r"([1-9][0-9]*),([1-9][0-9]*)/")
   for iset in inset:
      outlist = []
      for i in range(0, len(inset[iset])):
         ofile = outset[iset][i].format(run, seqno)
         with open(ofile, "w") as ostr:
            for sl in slices:
               ifile  = "{0},{1}/".format(sl[0], sl[1]) + inset[iset][i]
               for lines in open(ifile):
                  ostr.write(lines)
         outlist.append(ofile)
      tarfile = tarset[iset].format(run, seqno)
      cmd = subprocess.Popen(["tar", "zcf", tarfile] + outlist,
                             stderr=subprocess.PIPE)
      elog = cmd.communicate()
      if cmd.returncode != 0:
         for eline in elog[1].decode("ascii").split('\n'):
            badslice = slicepatt.search(eline)
            if badslice:
               badslices.append("{0},{1}".format(badslice.group(1),
                                                 badslice.group(2)))
            sys.stderr.write(eline + '\n')
         sys.stderr.write("Error on output file {0}".format(tarfile) +
                          " - job logs tarballing failed!\n")
         sys.stderr.flush()
         continue
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      upload(tarfile, odir)
   return badslices

# default action is to exit on the first error

while True:
   resp = next()
   if resp != 0:
      print("next() returns", resp)
      continue
   sys.exit(resp)
