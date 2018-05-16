# gluex-osg-jobscripts
Collection of template scripts useful as a starting point for running GlueX simulation / analysis on the Open Science Grid.

1. **osg-container.sh** - bash script which takes any bash command as argument[s], and executes it inside the singularity container used for Gluex production on the osg. This is a simple wrapper script that discovers if it is running inside the container, and if not, starts the container first, then proceeds to execute the given command. This script is the main executable for osg production jobs, but it also provides a useful tool for testing job scripts (see next item) prior to submitting to the grid. This script requires that the /cvmfs filesystem is present on the system where it runs. You will need to customize this script to the desired version of Gluex software for your work, and also to set the name of your grid proxy.

2. **osg-nocontainer.sh** - bash script which takes any bash command as argument[s], and executes it as if it were inside the singularity container, without requiring singularity to be installed. This is useful as a replacement for osg-container.sh on nodes that have access to the singularity container image, but do not have singularity installed. For this to script to succeed, you need first to run "osg-container.sh make.env" on a node where singularity is installed to generate the environment script approrpiate for the desired version of the Gluex software that you want to run. Once this env script is present, any executable binaries that were built to run inside the container are runnable from within this faux-container script. You just need to run them using the osg-container-helper.sh script, as in the example "./osg-container-helper.sh mcsmear". An environment variable OSG_CONTAINER_HELPER set to osg-contrainer-helper.sh in the osg-container.sh context, and set to null in the osg-container.sh context, is provided to make it possible to make job scripts that work in either context without modification. This script requires that the /cvmfs filesystem is present on the system where it runs, but singularity does not need to be present. It can be easily customized to use a copy of the singularity container somewhere else in the filesystem by changing the root directory /cvmfs in the script header.

3. **osg-container-helper.sh** - bash script which can run any binary that was built to run inside a standard Gluex singularity container, and run in outside the container context, provided that the container image is present on the filesystem, by default assumed to be found in the usual place under /cvmfs.  It takes any binary executable as its first argument, followed by any number of commandline arguments, and executes it as if it were run inside the singularity container used for Gluex production on the osg. 

4. **gridjob-template.py** - python script which contains the commands for executing a Gluex simulation and/or analysis job inside the Gluex singularity container. This template contains a minimal set of python functions to perform all of the steps of a simulation job, from Monte Carlo generation to final analysis and generation of root histograms. The user is expected to rename this script to something specific to the job it is intended to perform, and modify the header to include descriptive text regarding the job.

# Usage
If any of the above scripts is run without any arguments (or with -h or --help or -? as arguments) then the script exits immediately after printing a usage synopsis. The following are some sample command that illustrate how the scripts work when everything is set up correctly in your environment.

```
$ chmod +x osg-container.sh
$ ./osg-container.sh ls
Starting up container on gluex.phys.uconn.edu
Linux gluex.phys.uconn.edu 2.6.32-696.1.1.el6.x86_64 #1 SMP Tue Apr 11 17:13:24 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux
mygridjob.logs  mygridjob.py  osg-container.sh
Job container exited with code 0
```

```
$ ./osg-container.sh which root
Starting up container on gluex.phys.uconn.edu
Linux gluex.phys.uconn.edu 2.6.32-696.1.1.el6.x86_64 #1 SMP Tue Apr 11 17:13:24 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux
/usr/local/root/6.08.00/bin/root
Job container exited with code 0
```

```
$ ./osg-container.sh bash
Starting up container on gluex.phys.uconn.edu
Linux gluex.phys.uconn.edu 2.6.32-696.1.1.el6.x86_64 #1 SMP Tue Apr 11 17:13:24 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux
Singularity.gluex:latest> 
```

The above command start an interactive shell inside the container and waits for the user to type commands.

```
$ cp gridjob-template.py mygridjob.py
$ vim mygridjob.py [customize the script header, check that the functions do what you want]
$ ./mygridjob.py submit
$ condor_q

-- Schedd: scosg16.jlab.org : &lt;129.57.xx.yy:9615?...
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD               
1156.0   jonesrt         6/23 14:16   0+01:07:33 R  0   976.6 osg-container.sh .
1156.1   jonesrt         6/23 14:16   0+01:07:33 R  0   976.6 osg-container.sh .
1156.2   jonesrt         6/23 14:16   0+01:06:58 R  0   976.6 osg-container.sh .
...
```

The above command creates a new condor submit file in directory &lt;mygridjob>.logs, consisting of the full statistics contained in your &lt;mygridjob>.py sliced into a discrete number of grid-sized processes, and submits them all to the condor batch system for execution on the osg. The "condor_q" command can then be executed from time to time to monitor the progress of the job. As the slices complete, the output files show up in the cwd directory from which the "./mygridjob.py submit" command was issued, while the job logs are sent to subdirectory &lt;mygridjob>.logs, where &lt;mygridjob> is whatever name you used in the "cp" command.
