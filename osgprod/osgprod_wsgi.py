#!/usr/bin/env python3
#
# osgprod_wsgi.py - work dispatcher wsgi script for the osgprod
#                   Gluex grid raw data production environment.
#
# author: richard.t.jones at uconn.edu
# version: november 16, 2020
#

import re
import sys
import psycopg2

dbserver = "cn445.storrs.hpc.uconn.edu"
dbname = "osgprod"
dbuser = "gluex"
dbpass = "slicing+dicing"
magic_words = "good+curry"
documentroot = "/var/www/html"

def query_parameters(query):
   """
   Scan the query string for url-encoded parameters passed
   by the client and return them in a dict object with both
   key and value stored as strings.
   """
   pars = {}
   for keyval in query.rstrip(';& ').split('&'):
      if '=' in keyval:
         key,val = keyval.split('=')
         pars[key] = val
   return pars

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

def checkout_workscript(environ, output):
   """
   Checks out the next slice of work from the osgprod
   database and returns a job workscript for a new slice.
   """
   pars = query_parameters(environ["QUERY_STRING"])
   client = environ["REMOTE_ADDR"]
   cluster = pars["cluster"]
   process = pars["process"]
   project = pars["project"]
   cpus = int(pars["cpus"])
   xrootdprefix = ""
   workersubnet = ""
   workscript = ""
   maxblockspercore = 1
   
   with db_connection() as conn:
      with conn.cursor() as curr:
         curr.execute("""SELECT id, projectname, workersubnet,
                                workscript, xrootdprefix,
                                maxblockspercore
                         FROM projects
                         WHERE %s = projectname
                         AND %s LIKE workersubnet;
                      """, (project, client))
         row = curr.fetchone()
         iproject = int(row[0])
         xrootdprefix = row[4]
         workscript = row[3]
         workscriptpath = documentroot + "/" + project + "/" + row[3]
         maxblockspercore = int(row[5])
   
         curr.execute("""SELECT jobs.id, jobs.cluster, jobs.process,
                                jobs.nstarts, projects.projectname
                         FROM jobs LEFT JOIN projects
                         ON projects.id = jobs.project
                         WHERE jobs.cluster = %s
                         AND jobs.process = %s
                         AND projects.projectname = %s
                         AND jobs.endtime IS NOT NULL
                         AND jobs.exitcode IS NOT NULL
                         FOR UPDATE OF jobs;
                      """, (cluster, process, project))
         if curr.fetchone():
            output.append("echo Job already completed, quitting.")
            return "200 OK"
         curr.execute("""SELECT jobs.id, jobs.cluster, jobs.process,
                                jobs.nstarts, projects.projectname
                         FROM jobs LEFT JOIN projects
                         ON projects.id = jobs.project
                         WHERE jobs.cluster = %s
                         AND jobs.process = %s
                         AND projects.projectname = %s
                         AND jobs.endtime IS NULL
                         AND jobs.exitcode IS NULL
                         FOR UPDATE OF jobs;
                      """, (cluster, process, project))
         row = curr.fetchone()
         if row:
            ijob = int(row[0])
            nstarts = int(row[3])
         else:
            curr.execute("""INSERT INTO jobs 
                            (project, cluster, process)
                            VALUES (%s, %s, %s)
                            RETURNING id;
                         """, (iproject, cluster, process))
            row = curr.fetchone()
            ijob = int(row[0])
            nstarts = 0
         curr.execute("SELECT TIMEZONE('GMT', NOW());")
         now = curr.fetchone()[0]
         nstarts += 1
         curr.execute("""UPDATE jobs SET script = %s,
                                         worker = %s,
                                         ncpus = %s,
                                         nstarts = %s,
                                         starttime = %s
                         WHERE id = %s;
                      """, (workscript, client, cpus, nstarts, now, ijob))

   with db_connection() as conn:
      with conn.cursor() as curr:
         curr.execute("""SELECT id, ijob, iraw, block1, block2
                         FROM slices
                         WHERE ijob = %s;
                      """, (ijob,))
         row = curr.fetchone()
         if row:
            islice = int(row[0])
            iraw = int(row[2])
            block1 = int(row[3])
            block2 = int(row[4])
            lastblock = block2
         else:
            curr.execute("""SELECT id, ijob, iraw, block1, block2
                            FROM slices
                            WHERE ijob ISNULL
                            ORDER BY id
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED;
                         """)
            row = curr.fetchone()
            if len(row) == 0:
               output.append("echo No work left to do, quitting.")
               return "200 OK"
            islice = int(row[0])
            iraw = int(row[2])
            block1 = int(row[3])
            block2 = int(row[4])
            lastblock = block1 + maxblockspercore * cpus
            lastblock = block2 if lastblock > block2 else lastblock
            curr.execute("""UPDATE slices SET 
                            ijob = %s, block2 = %s
                            WHERE id = %s;
                         """, (ijob, lastblock, islice))
            if lastblock < block2:
               curr.execute("""INSERT INTO slices 
                               (iraw, block1, block2)
                               VALUES (%s, %s, %s);
                            """, (iraw, lastblock, block2))
         curr.execute("""SELECT path
                         FROM rawdata
                         WHERE id = %s;
                      """, (iraw,))
         row = curr.fetchone()
         rawpath = row[0]

   inpat = re.compile("#input_eviofile_list=\"root://xrootd.server.dns"
                      + "/path/to/file.evio ...\"")
   outpat = re.compile("#output_filename=\"file.evio\"")
   for line in open(workscriptpath):
      if re.match(r"^# project:", line):
         line = "# project: {0}\n".format(project)
      elif re.match(r"^# cluster:", line):
         line = "# cluster: {0}\n".format(cluster)
      elif re.match(r"^# process:", line):
         line = "# process: {0}\n".format(process)
      elif re.match(r"^# nstarts:", line):
         line = "# nstarts: {0}\n".format(nstarts)
      elif re.match(r"^# ncpus:", line):
         line = "# ncpus: {0}\n".format(cpus)
      elif re.match(r"^# source:", line):
         line = "# source: {0}\n".format(workscript)
      elif re.match(r"^# started:", line):
         line = "# started: {0}\n".format(now)
      output.append(line)
      if inpat.match(line):
         output.append("input_eviofile_list=\"\\\n")
         for block in range(block1, lastblock):
            blocksuffix = "+{0},{1}".format(block, block+1)
            xrootdpath = re.sub(r"^/dcache", xrootdprefix, rawpath)
            output.append(xrootdpath + blocksuffix + " \\\n")
         output.append("\"\n")
      elif outpat.match(line):
         blocksuffix = "+{0},{1}".format(block1, lastblock)
         outputfile = re.sub(r"^.*/([^/]*).evio", r"\1", rawpath)
         outputfile += blocksuffix + ".evio"
         output.append("output_filename=\"" + outputfile + "\"\n")
   return "200 OK"

def return_workscript(environ, output):
   """
   Registers the exit code from a job that was previously
   given a slice of work.
   """
   pars = query_parameters(environ["QUERY_STRING"])
   client = environ["REMOTE_ADDR"]
   cluster = pars["cluster"]
   process = pars["process"]
   project = pars["project"]
   exitcode = pars["exitcode"]

   with db_connection() as conn:
      with conn.cursor() as curr:
         curr.execute("""SELECT jobs.id, jobs.worker, jobs.cluster,
                                jobs.process, projects.projectname
                         FROM jobs LEFT JOIN projects
                         ON projects.id = jobs.project
                         WHERE jobs.worker = %s
                         AND jobs.cluster = %s
                         AND jobs.process = %s
                         AND projects.projectname = %s
                         AND jobs.endtime ISNULL
                         AND jobs.exitcode ISNULL;
                      """, (client, cluster, process, project))
         try:
            row = curr.fetchone()
            ijob = int(row[0])
         except:
            output.append("Never heard of you.")
            return "200 OK"
         curr.execute("SELECT TIMEZONE('GMT', NOW());")
         now = curr.fetchone()[0]
         curr.execute("""UPDATE jobs SET
                         endtime = %s,
                         exitcode = %s
                         WHERE id = %s;
                      """, (now, exitcode, ijob))
   output.append("Got it.")
   return "200 OK"

def application(environ, start_response):
   """
   Within the mod_wsgi, processing of the HTTP GET request 
   enters here with environ containing the key information
   from the request, and start_response to be loaded with 
   the header that leads the output to be sent back to the
   client.
   """
   output = []
   pars = query_parameters(environ["QUERY_STRING"])
   if not "magic" in pars or pars["magic"] != magic_words:
      output = ["Be gone with your crazy magic!"]
      status = "200 OK"
   elif "PATH_INFO" in environ and environ["PATH_INFO"]:
      if environ["PATH_INFO"] == "/workscript.bash":
         try:
            status = checkout_workscript(environ, output)
         except:
            status = "400 Bad Request"
            output = [""]
      elif environ["PATH_INFO"] == "/workscript.exit":
         try:
            status = return_workscript(environ, output)
         except:
            status = "400 Bad Request"
            output = [""]
      else:
         output = ["Be gone with your unknown path "]
         status = "200 OK"
   else:
      output = ["Be gone with you."]
      status = "200 OK"
      for var in environ:
         output.append(var + ": " + str(environ[var]) + "\n")

   output_len = sum([len(out) for out in output])
   response_headers = [("Content-type", "text/plain"),
                       ("Content-Length", str(output_len))]
   start_response(status, response_headers)
   return output
