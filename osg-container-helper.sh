#!/bin/sh
#
# osg-container-helper.sh - helper script to allow users to run binary
#                           applications outside the Gluex container 
#                           that were built to run inside.
#
# Usage: osg-container-helper.sh <application_binary>
#
# Author: Richard.T.Jones at uconn.edu
# Version: May 15, 2018

container="/cvmfs/singularity.opensciencegrid.org/markito3/gluex_docker_devel:latest"
oasisroot="/cvmfs/oasis.opensciencegrid.org/gluex"
dockerimage="docker://markito3/gluex_docker_devel:latest"

if [[ -h /tmp/sing ]]; then
    true
else
    ln -s $container /tmp/sing
fi

if echo $1 | grep -q ^/; then
    app_binary=$1
else
    app_binary=`which $1`
    if [[ $? != 0 ]]; then
        echo "executable $1 not found in path"
        exit 1
    elif [[ ! -x $app_binary ]]; then 
        echo "$1 is not executable"
        exit 1
    elif file $app_binary | grep -qi elf; then
         true
    else
        echo "$1 is not a binary executable"
        exit 1
    fi
fi

/tmp/sing/lib64/ld-linux-x86-64.so.2 --library-path /tmp/sing/lib64:/tmp/sing/usr/lib64:/tmp/sing/usr/lib64/mysql:$LD_LIBRARY_PATH $app_binary
