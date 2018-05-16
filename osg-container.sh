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
version=2.34_jlab
context=variation:mc

# define the container context for running on osg workers

if [[ -L /group ]]; then
    echo "Job running on" `hostname`
    if [[ $1 = "make.env" ]]; then
        echo "#!/usr/bin/env -i" > make.env
        echo "source $bs/gluex_env_jlab.sh $dist/version_$version.xml" >> make.env
        echo "env > this.env" >> make.env
        bash make.env
        sort this.env \
        | awk '/^SHLVL/{next}/^_=/{next}/^PWD=/{next}/^OLDPWD/{next}/^PATH/{print $1":/bin";next}{print}'\
        > osg-nocontainer_$version.env \
        && echo "new container environment script osg-nocontainer_$version.env created."
        retcode=$?
        rm -rf make.env this.env
        exit $retcode
    fi
    [ -r .$userproxy ] && mv .$userproxy /tmp/$userproxy
    source $bs/gluex_env_jlab.sh $dist/version_$version.xml
    export RCDB_CONNECTION=sqlite:///$dist/rcdb.sqlite
    export JANA_CALIB_URL=sqlite:///$dist/ccdb.sqlite
    export JANA_CALIB_CONTEXT=$context
    export OSG_CONTAINER_HELPER=""
    $* && retcode=$?
    echo "Job finished with exit code" $retcode
    exit $retcode

elif [[ -L $container/group ]]; then
    echo "Starting up container on" `hostname`
    [ -r /tmp/$userproxy ] && cp /tmp/$userproxy .$userproxy
    exec singularity exec --containall --bind ${oasismount} --home `pwd`:/srv --pwd /srv ${container} \
    bash $0 $*

else
    echo "Job container not found on" `hostname`
fi
