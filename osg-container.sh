#!/bin/sh
#
# osg-container.sh - gluex job wrapper script for osg jobs
#
# This script contains GENERIC (non-job-specific) steps for getting
# set up inside the gluex container on an osg worker node and launching
# the job script. It should not normally be modified by the user.
#
# Usage: osg-container.sh <job script> [job script arguments]
#
# Author: Richard.T.Jones at uconn.edu
# Version: June 8, 2017

container="/cvmfs/singularity.opensciencegrid.org/rjones30/gluex:latest"
#oasismount="/cvmfs/oasis.opensciencegrid.org"
oasismount="/cvmfs"
dockerimage="docker://rjones30/gluex:latest"

# define the container context for running on osg workers

if [[ -f /environment ]]; then
    echo "Job running on" `hostname`
    uname -a
    source /environment
    unset CCDB_CONNECTION
    unset RCDB_CONNECTION
    $* && retcode=$?
    echo "Job finished with exit code" $retcode
    rm -rf *.sqlite
    exit $retcode

elif [[ -f $container/environment ]]; then
    echo "Starting up container on" `hostname`
    uname -a
    singularity exec --containall --bind ${oasismount} --home `pwd`:/srv --pwd /srv --scratch /tmp,/var/tmp ${container} \
    bash -c "source /environment && unset CCDB_CONNECTION && unset RCDB_CONNECTION && cd /srv && $*"
    retcode=$?
    echo "Job container exited with code" $retcode
    rm -rf *.sqlite
    exit $retcode

else
    echo "Job container not found on" `hostname`
fi
