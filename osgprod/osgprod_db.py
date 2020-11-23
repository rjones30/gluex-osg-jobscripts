#!/usr/bin/env python3
#
# osgprod_db.py - functions for creating the job slicing database
#                 for Gluex production of raw data on the osg.
#
# author: richard.t.jones at uconn.edu
# version: november 15, 2020
#
# Run this script to initialize an empty jobs slice database
# after you have completed the following step on the server.
#
#   CREATE DATABASE osgprod;
#

import os
import re
import struct
import psycopg2

dbserver = "cn445.storrs.hpc.uconn.edu"
dbname = "osgprod"
dbuser = "gluex"
dbpass = "slicing+dicing"

def create_table_rawdata(delete=False):
   with connection.cursor() as cursor:
      if delete:
         cursor.execute("DROP TABLE rawdata;")
      cursor.execute("""CREATE TABLE rawdata
                        (id          SERIAL  PRIMARY KEY     NOT NULL,
                         run         INT                     NOT NULL,
                         seqno       INT                     NOT NULL,
                         path        TEXT                    NOT NULL,
                         nbytes      BIGINT                  NOT NULL,
                         nblocks     INT                     NOT NULL,
                         UNIQUE(run, seqno),
                         UNIQUE(path));
                     """)

def create_table_jobs(delete=False):
   with connection.cursor() as cursor:
      if delete:
         cursor.execute("DROP TABLE jobs;")
      cursor.execute("""CREATE TABLE jobs
                         (id          SERIAL  PRIMARY KEY     NOT NULL,
                          project     INT     REFERENCES projects(id),
                          script      TEXT,
                          worker      TEXT,
                          cluster     INT,
                          process     INT,
                          ncpus       INT,
                          nstarts     INT,
                          starttime   TIMESTAMP WITH TIME ZONE,
                          endtime     TIMESTAMP WITH TIME ZONE,
                          exitcode    INT,
                          UNIQUE(project, cluster, process));
                     """)

def create_table_projects(delete=False):
   with connection.cursor() as cursor:
      if delete:
         cursor.execute("DROP TABLE projects;")
      cursor.execute("""CREATE TABLE projects
                        (id               SERIAL  PRIMARY KEY     NOT NULL,
                         projectname      TEXT                    NOT NULL,                  
                         workersubnet     TEXT                    NOT NULL,
                         workscript       TEXT                    NOT NULL,
                         xrootdprefix     TEXT                    NOT NULL,
                         maxblockspercore INT                     NOT NULL);
                     """)

def create_table_slices(delete=False):
   with connection.cursor() as cursor:
      if delete:
         cursor.execute("DROP TABLE slices;")
      cursor.execute("""CREATE TABLE slices
                        (id          SERIAL  PRIMARY KEY     NOT NULL,
                         ijob        INT     REFERENCES jobs(id),
                         iraw        INT     REFERENCES rawdata(id),
                         block1      INT                     NOT NULL,
                         block2      INT                     NOT NULL);
                     """)

def load_rawdata_files(dir):
   """
   Walks the entire directory tree under dir looking for raw data
   files, and all that it finds are entered into the rawdata table
   of the osgprod database, if they are not already there. If they
   are there, it verifies that the db values are correct.
   """
   with connection.cursor() as cursor:
      filecount = 0
      commit_every = 1
      rawpat = re.compile(r"hd_rawdata_([0-9]+)_([0-9]+).evio")
      for root, subdir, files in os.walk(dir):
         for file in files:
            print(file)
            param = rawpat.match(file)
            if param:
               run = int(param.group(1))
               seqno = int(param.group(2))
               path = os.path.join(os.path.abspath(root), file)
               nbytes = os.path.getsize(path)
               nblocks = 0
               offset = 0
               fevio = open(path, 'rb')
               while offset < nbytes:
                  try:
                     fevio.seek(offset, 0)
                     bhead = fevio.read(4)
                     wsize = struct.unpack(">I", bhead)
                     offset += wsize[0] * 4
                     nblocks += 1
                  except:
                     break
               cursor.execute("SELECT id,run,seqno,nbytes,nblocks " +
                              "FROM rawdata where path = %s;", (path,))
               rows = cursor.fetchall()
               if len(rows) == 0:
                  cursor.execute("INSERT INTO rawdata " +
                                 "(run,seqno,path,nbytes,nblocks)" +
                                 "VALUES (%s,%s,%s,%s,%s);",
                                 (run, seqno, path, nbytes, nblocks))
                  filecount += 1
                  if (filecount % commit_every) == 0:
                     connection.commit()
               elif len(rows) == 1:
                  if rows[0][1] != run:
                     print("Database run number check failed for", file,
                           "- database says run number should be", rows[0][1])
                  elif rows[0][2] != seqno:
                     print("Database sequence number check failed for", file,
                           "- database says sequence number should be", rows[0][2])
                  elif rows[0][3] != nbytes:
                     print("Database file size check failed for", file,
                           "- database says files size should be", rows[0][3])
                  elif rows[0][4] != nblocks:
                     print("Database block count check failed for", file,
                           "- database says block count should be", rows[0][4])
               else:
                  print("Duplicate entry in database entry for rawdata file", file)
   if (filecount % commit_every) != 0:
      connection.commit()
   return filecount

def load_slices(runs=(0,999999)):
   """
   Scan through the rawdata table in the db for runs in
   the range runs[0]...runs[1] and append lines to the
   slices table for each one. Keep in mind that running
   load_slices will cause all of the selected runs to
   be processed from scratch, possibly repeating rawdata
   slices that have already been processed.
   """
   with connection.cursor() as cursor:
      cursor2 = connection.cursor()
      cursor.execute("SELECT id,nblocks from rawdata;")
      while True:
         rows = cursor.fetchmany(100)
         if not rows:
            break
         for row in rows:
            cursor2.execute("""INSERT INTO slices
                               (iraw,block1,block2)
                               VALUES (%s,1,%s);
                            """,(row[0], row[1]))
         connection.commit()

def add_project(name, subnet, script, xrootd, maxblocks):
   """
   Add a new entry to the project database.
   """
   with connection.cursor() as cursor:
      cursor.execute("""INSERT INTO project (projectname,
                                             workersubnet,
                                             scripttemplate,
                                             xrootdserver,
                                             maxblockspercore)
                        VALUES (%s,%s,%s,%s);
                     """, (name, subnet, script, xrootd, maxblocks)) 
      connection.commit()

def db_close():
   if connection:
      cursor.close()
      connection.close()
      print("PostgreSQL connection is closed")

def db_open():
   conn = psycopg2.connect(user = dbuser,
                           password = dbpass,
                           host = dbserver,
                           port = "5432",
                           database = dbname)
   
   # Print PostgreSQL version
   with conn.cursor() as cursor:
      cursor.execute("SELECT version();")
      record = cursor.fetchone()
      print("Connected to ", record)
   return conn

connection = db_open()
