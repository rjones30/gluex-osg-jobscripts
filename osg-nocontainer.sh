#!/bin/sh
#
# osg-nocontainer.sh - gluex job wrapper script for osg jobs
#
# This script contains GENERIC (non-job-specific) steps for getting
# set up outside the gluex container on an osg worker node and launching
# the job script. It should not normally be modified by the user.
#
# Note: Because one is running outside the container, eg. on a host
# without singularity installed, there are certain restrictions on
# what the job can do. For example, to run an executable that was
# built to run inside the container, it must be started with a
# special command prefix defined in $OSG_CONTAINER_HELPER.
#
# Usage: osg-nocontainer.sh <job script> [job script arguments]
#
# Author: Richard.T.Jones at uconn.edu
# Version: June 8, 2017

container="/cvmfs/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest"
oasisroot="/cvmfs/oasis.opensciencegrid.org/gluex"
dockerimage="docker://markito3/gluex_docker_devel:latest"
userproxy=x509up_u$UID

bs=/group/halld/Software/build_scripts
dist=/group/halld/www/halldweb/html/dist
version=2.34_jlab
context=variation:mc

# define the container context for running on osg workers

if [[ -L $container/group ]]; then
    echo "Job running on" `hostname`
    if [[ -f osg-nocontainer_$version.env ]]; then
        tmpenv=/tmp/env$$
        sed "s|/group/halld|$oasisroot/group/halld|g" osg-nocontainer_$version.env > $tmpenv
        source $tmpenv
        rm -f $tmpenv
    else
        echo "Error in osg-nocontainer.sh - "
        echo "  prebuilt container environment script osg-nocontainer_$version.env not found"
        echo "  You must build this script before using osg-nocontainer.sh with this version."
        exit 3
    fi
    export RCDB_CONNECTION=sqlite:///$container/$dist/rcdb.sqlite
    export JANA_CALIB_URL=sqlite:///$container/$dist/ccdb.sqlite
    export JANA_CALIB_CONTEXT=$context
    export OSG_CONTAINER_HELPER=`pwd`/osg-container-helper.sh
    $* && retcode=$?
    echo "Job finished with exit code" $retcode
    exit $retcode

else
    echo "Job container not found on" `hostname`
fi

