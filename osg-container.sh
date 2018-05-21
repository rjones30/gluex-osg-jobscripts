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
context="variation=mc calibtime=2018-05-21"

# define the container context for running on osg workers

if [[ -L /group ]]; then
    echo "Job running on" `hostname`
    echo "=== Contents of /cvmfs/oasis.opensciencegrid.org/gluex/update.details: ==="
    cat /cvmfs/oasis.opensciencegrid.org/gluex/update.details
    echo "=========================================================================="
    if [[ $1 = "make.env" ]]; then
        echo "#!/usr/bin/env -i" > make.env
        echo "source $bs/gluex_env_jlab.sh $dist/version_$version.xml" >> make.env
        echo "env > this.env" >> make.env
        bash make.env
        sort this.env \
        | awk '/^SHLVL/{next}/^_=/{next}/^PWD=/{next}/^OLDPWD/{next}{print}' \
        | awk '/^PATH/{print $1":/bin";next}{print}' \
        | awk -F= '/^PYTHONPATH/{ppath=$2;next}/^HALLD_HOME=/{hhome=$2}
                   /^BMS_OSNAME=/{osname=$2}{print}
                   END{print "PYTHONPATH="ppath""hhome"/"osname"/python2"}' \
        > osg-nocontainer_$version.env \
        && echo "new container environment script osg-nocontainer_$version.env created."
        retcode=$?
        rm -rf make.env this.env
        exit $retcode
    fi
    [ -r .$userproxy ] && mv .$userproxy /tmp/$userproxy
    source $bs/gluex_env_jlab.sh $dist/version_$version.xml
    export RCDB_CONNECTION=sqlite:///$dist/rcdb.sqlite
    export CCDB_CONNECTION=sqlite:///$dist/ccdb.sqlite
    export JANA_GEOMETRY_URL=ccdb://GEOMETRY/main_HDDS.xml
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
    exit 9
fi
