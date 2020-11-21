# osgprod scripts
Collection of python scripts that were developed to help with projection of raw GlueX data on the Open Science Grid

1. **osgprod_db.py** - python script to help create and configure the work slicing database

2. **osgprod_wsgi.py** - Apache wsgi gateway module that interfaces between running jobs and the database

3. **osgprod_work.bash** - production script that runs the reconstruction on the osg workers

4. **osgprod_exec.sh** - wrapper script to submit to the osg using condor_submit

# Dependencies

The database should be created on a running postgres server. It has been tested and verified to work with postgres version 10.
The name of the database and the user/password needed to access it are stored in plain text in the osgprod_db and osgprod_wsgi
scripts, so these scripts should be readonly if the user wants to keep these a secret. Their values are user-defined.

The osgprod_wsgi script has been designed to run within an Apache web server using the mod_wsgi plugin. This plugin is available
as a standard system package under Redhat/Centos 7. The following snippet from /etc/httpd/conf.d/ssl.conf on a standard RHEL7
system illustrates how to configure the osgprod_wsgi script after copying it under the standard /var/www/wsgi-scripts location.

```
# Setup for osgprod WSGI module [Richard Jones, 11-16-2020]
WSGIScriptAlias /osgprod /var/www/wsgi-scripts/osgprod_wsgi.py

<Directory "/var/www/wsgi-scripts">
<IfVersion < 2.4>
   Order allow,deny
   Allow fromall
</IfVersion>
<IfVersion >= 2.4>
   Require all granted
</IfVersion>
</Directory>
```

No client access control is presently supported, so any client with an internet connection can issue requests
to this server. I have not done a complete assessment of its vulnerability to attack, but the protocol is
quite restrictive and the database updates are checked against script injection using standard postgres
parameter replacement.

# Usage

After the postgres database server is running and an empty database has been created, initialize the database
tables using commands like the following.

```
$ python3
>>> import osgprod_db
>>> osgprod_db.create_table_projects()
>>> osgprod_db.create_table_rawdata()
>>> osgprod_db.create_table_jobs()
>>> osgprod_db.create_table_slices()
>>> osgprod_db.add_project(projectname, subnet_pattern, workscript, xrootd_prefix, maxblockspercore)
>>> load_rawdata_files(my_rawdata_dir)
>>> load_slices()
```

The add_project line above assumes that you have customized the osgprod_work.bash script to your liking
and installed it under your DocumentRoot on your Apache server, default location /var/www/html/projectname.
Use the psql command-line interface or your favorite web postgres database admin tool to view and modify
the database tables to you liking. Then install osgprod_wsgi.py in the appropriate location under your
DocumentRoot of your Apache server and make sure it is working. Accessing a url like the following 
should return a bash script in plain text that could be executed to perform a slice of work.

```
wget https://your.apache.server/osgprod/workscript.bash?project=myproject&cluster=0&process=0&magic=my+magic;
```

Repeated reloads of the same web page should show the start time and nstarts fields in the comments
header incrementing to the current time and request count. Once these tests are working, you should
reset your database to remove the dummy job entries you have created in this test. Once this is done,
customize the osgprod_exec.sh script to point to your Apache server and project name, and submit some jobs.

Richard.T.Jones at uconn.edu
