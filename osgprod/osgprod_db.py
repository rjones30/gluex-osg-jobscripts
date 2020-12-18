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

def db_close():
   if dbconnection:
      dbconnection.close()
      print("PostgreSQL connection is closed")

def create_table_rawdata(delete=False):
   with db_connection() as conn:
      with conn.cursor() as cursor:
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
   with db_connection() as conn:
      with conn.cursor() as cursor:
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
   with db_connection() as conn:
      with conn.cursor() as cursor:
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
   with db_connection() as conn:
      with conn.cursor() as cursor:
         if delete:
            cursor.execute("DROP TABLE slices;")
         cursor.execute("""CREATE TABLE slices
                           (id          SERIAL  PRIMARY KEY     NOT NULL,
                            ijob        INT     REFERENCES jobs(id),
                            iraw        INT     REFERENCES rawdata(id),
                            block1      INT                     NOT NULL,
                            block2      INT                     NOT NULL);
                        """)

def create_table_bindings(delete=False):
   with db_connection().cursor() as conn:
      with conn.cursor() as cursor:
         if delete:
            cursor.execute("DROP TABLE bindings;")
         cursor.execute("""CREATE TABLE bindings
                           (id          SERIAL  PRIMARY KEY     NOT NULL,
                            iraw        INT     REFERENCES rawdata(id),
                            starttime   TIMESTAMP WITH TIME ZONE,
                            endtime     TIMESTAMP WITH TIME ZONE,
                            exitcode    INT,
                            details     TEXT,
                            UNIQUE(iraw));
                        """)

def add_project(name, subnet, script, xrootd, maxblocks):
   """
   Add a new entry to the project database.
   """
   with db_connection() as conn:
      with conn.cursor() as cursor:
         cursor.execute("""INSERT INTO project (projectname,
                                                workersubnet,
                                                scripttemplate,
                                                xrootdserver,
                                                maxblockspercore)
                           VALUES (%s,%s,%s,%s);
                        """, (name, subnet, script, xrootd, maxblocks)) 

def load_rawdata_files(dir):
   """
   Walks the entire directory tree under dir looking for raw data
   files, and all that it finds are entered into the rawdata table
   of the osgprod database, if they are not already there. If they
   are there, it verifies that the db values are correct.
   """
   with db_connection() as conn:
      with conn.cursor() as cursor:
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
                        conn.commit()
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
   with db_connection() as conn:
      with conn.cursor() as cursor:
         cursor2 = conn.cursor()
         cursor.execute("""SELECT id,nblocks from rawdata
                           WHERE run >= %s AND run < %s;
                        """, (runs[0], runs[1]))
         while True:
            rows = cursor.fetchmany(100)
            if not rows:
               break
            for row in rows:
               cursor2.execute("""INSERT INTO slices
                                  (iraw,block1,block2)
                                  VALUES (%s,1,%s);
                               """,(row[0], row[1]))
            conn.commit()

def recycle_slices(runs=(0,999999)):
   """
   Scan through the bindings table in the db for slices
   that failed to return errorcode = 0. For those with a
   run number in the range of runs, remove the jobs id
   associated with that slice so that the work dispatcher
   will rerun production on that slice, then remove the
   failed bindings entry so that binding will be tried
   again as soon as the production finishes.
   """
   badslices = {}
   with db_connection() as conn:
      with conn.cursor() as cursor:
         cursor.execute("""SELECT bindings.id,
                                  bindings.iraw,
                                  bindings.details
                           FROM bindings
                           JOIN rawdata
                           ON rawdata.id = bindings.iraw
                           WHERE rawdata.run >= %s
                           AND rawdata.run < %s
                           AND bindings.exitcode != 0;
                        """, (runs[0], runs[1]))
         for row in cursor.fetchall():
            badset = set()
            for slix in row[2].split(':'):
               blix = slix.split(',')
               badset.add((int(blix[0]), int(blix[1])))
            if len(badset) > 0:
               iraw = int(row[1])
               badslices[iraw] = badset

   recycled = 0
   with db_connection() as conn:
      with conn.cursor() as cursor:
         for iraw in badslices:
            for blix in badslices[iraw]:
               cursor.execute("""UPDATE slices
                                 SET ijob = NULL
                                 WHERE iraw = %s
                                 AND block1 = %s
                                 AND block2 = %s;
                              """, (iraw, blix[0], blix[1]))
               recycled += 1
            cursor.execute("""DELETE FROM bindings
                              WHERE iraw = %s;
                           """, (iraw,))
   return recycled
