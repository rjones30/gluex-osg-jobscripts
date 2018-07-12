#!/bin/bash
#
# osg-jobwrapper.bash - osg job wrapper script for GlueX jobs.
#
# author: richard.t.jones at uconn.edu
# version: may 5, 2018
#
# usage: osg-jobwrapper.bash <user_job_script> [ <arguments> ]

function usage() {
    echo "Usage: osg-jobwrapper.bash <user_job_script> [ <arguments> ]"
    exit 1
}

echo "===GlueX osg job log started" `date` "on host" `hostname -f` "("`hostname -i`")==="

"$@"
exitcode=$?

if [ $exitcode != 0 ]; then
    echo 
    echo "Job failed with exit code $exitcode, sending kill to glidein on the way out."
    pid=$$
    while [ $pid != 1 ]; do 
        pid=`ps -ho ppid $pid`
        ps -hfw $pid
        if ps -ho cmd $pid | grep -q '[/ ]condor_master '; then
            echo kill $pid
            kill $pid 2>&1
            killed=$pid
            break
        fi
    done
    if [ "$killed" != "" ]; then
        echo "Nighty, night."
    else
        echo "Unable to kill glidein, sleeping for 20 minutes to block retries."
    fi
    myname=`hostname -f`
    if [ -z "$myname" ]; then
        date > unknown_host.bad
    else
        date > $myname.bad
    fi
    sleep 1200
    echo
fi

echo "===GlueX osg job log ended" `date` "on host" `hostname -f` "("`hostname -i`")==="
exit $exitcode
