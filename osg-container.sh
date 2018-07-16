#!/bin/bash
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

dockerimage="docker://markito3/gluex_docker_devel:latest"
container="/cvmfs/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest"
oasismount="/cvmfs"

if [[ -n "$OSG_GLUEX_CONTAINER" ]]; then
    container=$OSG_GLUEX_CONTAINER
fi
if [[ -n "$OSG_GLUEX_SOFTWARE" ]]; then
    oasismount=$OSG_GLUEX_SOFTWARE
fi
     
oasisprefix="oasis.opensciencegrid.org/gluex"
oasisroot="$oasismount/$oasisprefix"
userproxy=x509up_u$UID

bs=/group/halld/Software/build_scripts
dist=/group/halld/www/halldweb/html/dist
version=2.29_jlab
context="variation=mc calibtime=2018-05-21"

# define the container context for running on osg workers

# check if we are already inside the container -- no foolproof way to do this!!
if [[ ! -d /boot ]]; then
    echo "Job running on" `hostname`
    echo "=== Contents of $oasisroot/update.details: ==="
    cat $oasisroot/update.details
    echo "=========================================================================="
    if [[ $1 = "make.env" ]]; then
        echo "#!/bin/bash" > make.env
        echo '[ -z "$CLEANENV" ] && exec /bin/env -i CLEANENV=1 /bin/sh "$0" "$@"' >> make.env
        echo "unset CLEANENV" >> make.env
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
    elif [[ $1 = "make.tgz" ]]; then
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
        | awk -F[=:] '{for(i=2;i<=NF;++i){print $i}}' \
        > make.list

        # Here is where we populate the stripped-down container tarball,
        # so add to this list any directories that need to be included.
        echo "$oasisprefix/update.details" > make.tgz
        echo "$oasisprefix$dist" >> make.tgz
        echo "$oasisprefix/xrootd" >> make.tgz
        echo "$oasisprefix/Diracxx" >> make.tgz
        echo "$oasisprefix/HDGeant4/g4py" >> make.tgz
        echo "$oasisprefix/HDGeant4/bin/Linux-g++" >> make.tgz
        echo "$oasisprefix/HDGeant4/tmp/Linux-g++/hdgeant4" >> make.tgz
        for ford in `cat make.list`; do
            if echo $ford | grep -q "^/group/halld"; then
                if [[ -f $ford || -h $ford || -d $ford ]]; then
                    basedir=`echo $ford | awk -F/ '{if(NF>7){print $1"/"$2"/"$3"/"$4"/"$5"/"$6"/"$7"/"$8}}'`
                    if [[ -n "$basedir" ]]; then
                        echo $oasisprefix$basedir >> make.tgz
                    elif echo $ford | grep -q '/build_scripts$'; then
                        echo $oasisprefix$ford >> make.tgz
                    else
                        echo $ford >> make.lost
                    fi
                fi
            fi
        done
        tar zcf osg-nocontainer_$version.tgz -C $oasismount `sort -u make.tgz`
        retcode=$?
        rm -rf make.env this.env make.list make.tgz make.lost update.details
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
    if [[ -d $oasisroot/xrootd ]]; then
        export XROOTD_HOME=$oasisroot/xrootd/3.3.2
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$XROOTD_HOME/lib64
        export LD_PRELOAD=$XROOTD_HOME/lib64/libXrdPosixPreload.so
    else
        unset LD_PRELOAD
    fi
    $* ; retcode=$?
    echo "Job finished with exit code" $retcode
    exit $retcode

elif [[ -L $container/group || -d $container/group ]]; then
    echo "Starting up container on" `hostname`
    [ -r /tmp/$userproxy ] && cp /tmp/$userproxy .$userproxy
    exec singularity exec --containall --bind ${oasismount}:/cvmfs --home `pwd`:/srv --pwd /srv ${container} \
    bash $0 $*

else
    echo "Job container not found on" `hostname`
    echo "Hint: Look at http://zeus.phys.uconn.edu/halld/containers"
    exit 9
fi
