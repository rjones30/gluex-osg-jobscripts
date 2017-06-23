# gluex-osg-jobscripts
Collection of template scripts useful as a starting point for running GlueX simulation / analysis on the Open Science Grid.

1. **osg-container.sh** - bash script which takes any bash command as argument[s], and executes it inside the singularity container used for Gluex production on the osg. This is a simple wrapper script that discovers if it is running inside the container, and if not, starts the container first, then proceeds to execute the given command. This script is the main executable for osg production jobs, but it also provides a useful tool for testing job scripts (see next item) prior to submitting to the grid. This script requires that the /cvmfs filesystem is present on the system where it runs.

2. **gridjob-template.py** - python script which contains the commands for executing a Gluex simulation and/or analysis job inside the Gluex singularity container. This template contains a minimal set of python functions to perform all of the steps of a simulation job, from Monte Carlo generation to final analysis and generation of root histograms. The user is expected to rename this script to something specific to the job it is intended to perform, and modify the header to include descriptive text regarding the job.

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
```

The above command creates a new condor submit file in directory <mygridjob>.logs, consisting of the full statistics contained in your <mygridjob>.py sliced into a discrete number of grid-sized processes, and submits them all to the condor batch system for execution on the osg. The "condor_q" command can then be executed from time to time to monitor the progress of the job. As the slices complete, the output files show up in the cwd directory from which the "./mygridjob.py submit" command was issued, while the job logs are sent to subdirectory <mygridjob>.logs, where <mygridjob> is whatever name you used in the "cp" command.
