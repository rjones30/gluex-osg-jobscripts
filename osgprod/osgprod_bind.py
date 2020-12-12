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

dbserver = "cn445.storrs.hpc.uconn.edu"
dbname = "osgprod"
dbuser = "gluex"
dbpass = "slicing+dicing"

xrootd_url = "root://cn442.storrs.hpc.uconn.edu"
srm_url = "srm://cn446.storrs.hpc.uconn.edu:8443"
input_area = "/gluex/resilient"
output_area = "/gluex/resilient/recon/ver01"

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

def create_table_bindings(delete=False):
   with db_connection().cursor() as conn:
      with conn.cursor() as curs:
         if delete:
            curs.execute("DROP TABLE bindings;")
         curs.execute("""CREATE TABLE bindings
                         (id          SERIAL  PRIMARY KEY     NOT NULL,
                          iraw        INT     REFERENCES rawdata(id),
                          starttime   TIMESTAMP WITH TIME ZONE,
                          endtime     TIMESTAMP WITH TIME ZONE,
                          exitcode    INT,
                          UNIQUE(iraw));
                      """)

def init_output_area(area):
   """
   Creates the directory structure in output_area to receive the
   merged results from this module.
   """
   return subprocess.call(["gfal-mkdir", srm_url + area],
                          stdout = subprocess.DEVNULL,
                          stderr = subprocess.STDOUT)

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
                                   rawdata.nblocks,slices.block1,slices.block2,
                                   jobs.cluster,jobs.process
                            FROM rawdata
                            LEFT JOIN bindings
                            ON bindings.iraw = rawdata.id
                            INNER JOIN slices
                            ON slices.iraw = rawdata.id
                            INNER JOIN jobs
                            ON slices.ijob = jobs.id
                            WHERE bindings.id IS NULL
                            AND jobs.exitcode = 0
                            ORDER BY rawdata.id,slices.block1
                            LIMIT 100;
                         """)
            blocks2do = {}
            for row in curs.fetchall():
               i = int(row[0])
               if i != iraw:
                  if len(blocks2do) == 1:
                     break
                  else:
                     run = int(row[1])
                     seqno = int(row[2])
                     nblocks = int(row[3])
                     blocks2do = set(range(0,nblocks))
                     slices = []
                     iraw = i
               block1 = int(row[4])
               block2 = int(row[5])
               blocks2do ^= set(range(block1,block2))
               cluster = int(row[6])
               process = int(row[7])
               slices.append((block1,block2,cluster,process))
            if len(blocks2do) != 1:
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
   for sl in slices:
      sdir = str(sl[0]) + "," + str(sl[1])
      os.mkdir(sdir)
      tarfile = "job_{0}_{1}.tar.gz".format(sl[2], sl[3])
      tarpath = input_area + "/" + tarfile
      try:
         subprocess.check_output(["gfal-copy", srm_url + tarpath,
                                  "file://" + os.getcwd() + "/" + tarfile])
      except:
         sys.stderr.write("Error -999 on rawdata id {0}".format(iraw) +
                          " - job output " + tarfile + " is missing!\n")
         sys.stderr.flush()
         with db_connection() as conn:
            with conn.cursor() as curs:
               curs.execute("SELECT TIMEZONE('GMT', NOW());")
               now = curs.fetchone()[0]
               curs.execute("""UPDATE bindings
                               SET endtime=%s, exitcode=%s
                               WHERE id = %s;
                            """, (now, -999, ibind))
         os.chdir("..")
         shutil.rmtree(workdir)
         return 1
      untar = ["tar", "zxf", tarfile, "-C", sdir]
      if subprocess.Popen(untar).wait() != 0:
         sys.stderr.write("Error -998 on rawdata id {0}".format(iraw) +
                          " - job output " + tarfile + " is not readable!\n")
         sys.stderr.flush()
         with db_connection() as comm:
            with conn.cursor() as curs:
               curs.execute("SELECT TIMEZONE('GMT', NOW());")
               now = curs.fetchone()[0]
               curs.execute("""UPDATE bindings
                               SET endtime=%s, exitcode=%s
                               WHERE id = %s;
                            """, (now, -998, ibind))
         os.chdir("..")
         shutil.rmtree(workdir)
         return 1
      else:
         os.remove(tarfile)

   exitcode = 0
   exitcode += merge_evio_skims(run, seqno, slices)
   exitcode += merge_hddm_output(run, seqno, slices)
   exitcode += merge_job_info(run, seqno, slices)
   exitcode += merge_root_histos(run, seqno, slices)
   with db_connection() as comm:
      with conn.cursor() as curs:
         curs.execute("SELECT TIMEZONE('GMT', NOW());")
         now = curs.fetchone()[0]
         curs.execute("""UPDATE bindings
                         SET endtime=%s, exitcode=%s
                         WHERE id = %s;
                      """, (now, exitcode, ibind))
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
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = ["{0},{1}/".format(sl[0], sl[1]) +
                inset[iset].format(run, seqno, sl[0], sl[1])
               for sl in slices
               ]
      cmd = ["eviocat", "-o", ofile] + ifiles
      if subprocess.Popen(cmd).wait() != 0:
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - evio file merging failed!\n")
         sys.stderr.flush()
         return -1 # output data files unreadable
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      init_output_area(odir)
      opath = odir + "/" + ofile
      subprocess.check_output(["gfal-copy", "-f",
                               "file://" + os.getcwd() + "/" + ofile,
                               srm_url + opath])
   return 0

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
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = ["{0},{1}/".format(sl[0], sl[1]) +
                inset[iset].format(run, seqno, sl[0], sl[1])
               for sl in slices
               ]
      cmd = ["hadd", ofile] + ifiles
      if subprocess.Popen(cmd).wait() != 0:
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - root file merging failed!\n")
         sys.stderr.flush()
         return -1
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      init_output_area(odir)
      opath = odir + "/" + ofile
      subprocess.check_output(["gfal-copy", "-f",
                               "file://" + os.getcwd() + "/" + ofile,
                               srm_url + opath])
   return 0

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
   for iset in inset:
      ofile = outset[iset].format(run, seqno)
      ifiles = []
      for sl in slices:
         ifile = "{0},{1}/".format(sl[0], sl[1]) +\
                 inset[iset].format(run, seqno, sl[0], sl[1])
         if iset == "converted_random" and not os.path.exists(ifile):
            continue # missing converted_random is not an error
         ifiles.append(ifile)
      cmd = ["hddmcat", "-o", ofile] + ifiles
      if subprocess.Popen(cmd).wait() != 0:
         sys.stderr.write("Error on output file {0}".format(ofile) +
                          " - hddm file merging failed!\n")
         sys.stderr.flush()
         return -1
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      init_output_area(odir)
      opath = odir + "/" + ofile
      subprocess.check_output(["gfal-copy", "-f",
                               "file://" + os.getcwd() + "/" + ofile,
                               srm_url + opath])
   return 0

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
      cmd = ["tar", "zcf", tarfile] + outlist
      if subprocess.Popen(cmd).wait() != 0:
         sys.stderr.write("Error on output file {0}".format(tarfile) +
                          " - job logs tarballing failed!\n")
         sys.stderr.flush()
         return -1
      odir = output_area + "/" + iset + "/{0:06d}".format(run)
      init_output_area(odir)
      opath = odir + "/" + tarfile
      subprocess.check_output(["gfal-copy", "-f",
                               "file://" + os.getcwd() + "/" + ofile,
                               srm_url + opath])
   return 0

# default action is to exit on the first error

while True:
   resp = next()
   if 0 * resp != 0:
      print("next() returns", resp)
      continue
   sys.exit(resp)
