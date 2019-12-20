#!/bin/bash
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

dockerimage="docker://markito3/gluex_docker_devel:latest"
#container="/cvmfs/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest"
container="/cvmfs/singularity.opensciencegrid.org/rjones30/gluex:latest"
oasismount="/cvmfs"

if [[ -n "$OSG_GLUEX_CONTAINER" ]]; then
    container=$OSG_GLUEX_CONTAINER/singularity.opensciencegrid.org/rjones30/gluex:latest
    #container=$OSG_GLUEX_CONTAINER/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest
fi
if [[ -n "$OSG_GLUEX_SOFTWARE" ]]; then
    oasismount=$OSG_GLUEX_SOFTWARE
fi
     
oasisprefix="oasis.opensciencegrid.org/gluex"
oasisroot="$oasismount/$oasisprefix"
userproxy=x509up_u$UID
     
bs=/group/halld/Software/build_scripts
dist=/group/halld/www/halldweb/html/dist
version=4.12.0
context="variation=mc"

# define the container context for running on osg workers

if [[ -d $container/group || -h $container/group ]]; then
    echo "Job running on" `hostname`
    echo "=== Contents of $oasisroot/gluex/update.details: ==="
    cat $oasisroot/update.details
    echo "=========================================================================="
    if [[ -f osg-nocontainer_$version.env ]]; then
        source /dev/stdin <<<$(cat osg-nocontainer_$version.env \
        | sed "s|/group/halld|$oasisroot/group/halld|g" \
        | awk '{print "export",$0}')
    else
        echo "Error in osg-nocontainer.sh - "
        echo "  prebuilt container environment script osg-nocontainer_$version.env not found"
        echo "  You must build this script before using osg-nocontainer.sh with this version."
        exit 3
    fi
    export RCDB_CONNECTION=sqlite:///$oasisroot/$dist/rcdb.sqlite
    export CCDB_CONNECTION=sqlite:///$oasisroot/$dist/ccdb.sqlite
    export JANA_GEOMETRY_URL=ccdb://GEOMETRY/main_HDDS.xml
    export JANA_CALIB_URL=sqlite:///$oasisroot/$dist/ccdb.sqlite
    export JANA_CALIB_CONTEXT=$context
    export OSG_CONTAINER_HELPER=`pwd`/osg-container-helper.sh
    if [[ -d $oasisroot/xrootd ]]; then
        export XROOTD_HOME=$oasisroot/xrootd/4.9.1/x86_64
        export PATH=$XROOTD_HOME/bin:$PATH
        export LD_LIBRARY_PATH=$XROOTD_HOME/lib64:$LD_LIBRARY_PATH
        export LD_PRELOAD=$XROOTD_HOME/lib64/libXrdPosixPreload.so
    else
        unset LD_PRELOAD
    fi
    $*
    retcode=$?
    echo "Job finished with exit code" $retcode
    exit $retcode

else
    echo "Job container not found on" `hostname`
    echo "Hint: Look at http://zeus.phys.uconn.edu/halld/containers"
    exit 9
fi
