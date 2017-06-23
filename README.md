# gluex-osg-jobscripts
Collection of template scripts useful as a starting point for running GlueX simulation / analysis on the Open Science Grid.

1. **osg-container.sh** - bash script which takes any bash command as argument[s], and executes it inside the singularity container used for Gluex production on the osg. This is a simple wrapper script that discovers if it is running inside the container, and if not, starts the container first, then proceeds to execute the given command. This script is the main executable for osg production jobs, but it also provides a useful tool for testing job scripts (see next item) prior to submitting to the grid. This script requires that the /cvmfs filesystem is present on the system where it runs.

2. **gridjob-template.py** - python script which contains the commands for executing a Gluex simulation and/or analysis job inside the Gluex singularity container. This template contains a minimal set of python functions to perform all of the steps of a simulation job, from Monte Carlo generation to final analysis and generation of root histograms. The user is expected to rename this script to something specific to the job it is intended to perform, and modify the header to include descriptive text regarding the job.

# Usage
Descriptive steps
