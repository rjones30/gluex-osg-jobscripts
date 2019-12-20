#!/bin/bash
#
# osg-container-helper.sh - helper script to allow users to run binary
#                           applications outside the Gluex container 
#                           that were built to run inside.
#
# Usage: osg-container-helper.sh <application_binary>
#
# Author: Richard.T.Jones at uconn.edu
# Version: May 15, 2018

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

tmpsing=`pwd`/.sing
if [[ -h $tmpsing ]]; then
    true
else
    ln -s $container $tmpsing
fi

if echo $1 | grep -q ^/; then
    app_binary=$1
elif [[ -z "$1" ]]; then
    echo "no executable specified"
    exit 1
else
    for path in $tmpsing/bin $tmpsing/sbin \
                $tmpsing/usr/bin $tmpsing/usr/sbin \
                $tmpsing/usr/local/bin $tmpsing/usr/local/sbin
     do
        if [[ -x $path/$1 ]]; then
            app_binary=$path/$1
            break
        fi
     done
    if [[ -z "$app_binary" ]]; then
        app_binary=`which $1`
    fi
    if [[ $? != 0 ]]; then
        echo "executable $1 not found in path"
        exit 1
    elif [[ ! -x $app_binary ]]; then 
        echo "$1 is not executable"
        exit 1
    elif head $app_binary | grep -q ^.ELF; then
         true
    else
        echo "$1 is not a binary executable"
        exit 1
    fi
fi
shift

$tmpsing/lib64/ld-linux-x86-64.so.2 --library-path $tmpsing/lib64:$tmpsing/usr/lib64:$tmpsing/usr/lib64/mysql:$LD_LIBRARY_PATH $app_binary $*
