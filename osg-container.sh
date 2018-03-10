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

container="/cvmfs/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest"
oasismount="/cvmfs"
dockerimage="docker://markito3/gluex_docker_devel:latest"
userproxy=x509up_u$UID

bs=/group/halld/Software/build_scripts
dist=/group/halld/www/halldweb/html/dist
version=2.29_jlab
context=variation:mc

# define the container context for running on osg workers

if [[ -L /group ]]; then
    echo "Job running on" `hostname`
    [ -r .$userproxy ] && mv .$userproxy /tmp/$userproxy
    source $bs/gluex_env_jlab.sh $dist/version_$version.xml
    export RCDB_CONNECTION=sqlite:///$dist/rcdb.sqlite
    export JANA_CALIB_URL=sqlite:///$dist/ccdb.sqlite
    export JANA_CALIB_CONTEXT=$context
    $* && retcode=$?
    echo "Job finished with exit code" $retcode
    exit $retcode

elif [[ -L $container/group ]]; then
    echo "Starting up container on" `hostname`
    [ -r /tmp/$userproxy ] && cp /tmp/$userproxy .$userproxy
    exec singularity exec --containall --bind ${oasismount} --home `pwd`:/srv --pwd /srv --scratch /tmp,/var/tmp ${container} \
    bash $0 $*

else
    echo "Job container not found on" `hostname`
fi
