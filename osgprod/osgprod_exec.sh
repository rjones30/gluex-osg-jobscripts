#!/bin/bash
#
# osgprod_exec.sh - grid node execution script for the osgprod
#                   Gluex grid raw data production environment.
#
# author: richard.t.jones at uconn.edu
# version: november 16, 2020
#
# usage: ./osgrod_exec.sh <cluster> <process> [<ncpus>]
#  where <cluster> and <process> are two numerical identifiers
#  that are used to uniquely identify a grid job within the
#  osgprod environment. Normally they correspond to the
#  condor job cluster and process number on the submit host.
#  If a third argument is given it must be the number of cores
#  available for this job, otherwise it defaults to 1.

osgprod_url="https://cn410.storrs.hpc.uconn.edu/osgprod"
magic_words="good+curry"
project="osg-11-2020"
output_collector="srm://cn446.storrs.hpc.uconn.edu:8443/gluex/resilient"
curl="curl -s -f --capath /etc/grid-security/certificates"

if [ $# = 3 ]; then
    ncpus=$3
elif [ $# = 2 ]; then
    ncpus=1
else
    echo "usage: ./osgprod_exec.py <cluster> <process> [<ncpus>]"
    exit 1
fi

export CLUSTER=$1
export PROCESS=$2
export NTHREADS=$ncpus

function error_exit {
    echo "error code $1"
    exit $1
}

function report_exit {
    echo -n "sending job report back to job dispatch..."
    $curl "$osgprod_url/workscript.exit?exitcode=$1&cluster=$CLUSTER&process=$PROCESS&project=$project&magic=$magic_words;" || error_exit $?
    echo
    exit 0
}

echo "Job $1.$2 is executing on" `hostname -f`
echo -n "fetching new job slice workscript from osgprod server..."
for retry in 0 1 2; do
    $curl -o workscript.bash "$osgprod_url/workscript.bash?cluster=$CLUSTER&process=$PROCESS&project=$project&cpus=$ncpus&magic=$magic_words;"
    retcode=$?
    if [ $retcode = 0 -a -r workscript.bash ]; then
        chmod +x workscript.bash
        break
    elif [ $retry = 2 ]; then
        echo "failed"
        error_exit $retcode
    fi
    sleep 1
done
echo "succeeded"

echo -n "executing workscript..."
./workscript.bash >workscript.stdout 2>workscript.stderr
retcode=$?
if [ $retcode = 137 ]; then
    echo -n "job was killed..."
    error_exit $retcode
elif [ $retcode != 0 ]; then
    echo "failed with exit code $retcode"
    flog=job_${1}_${2}.flog
    echo "======================" > $flog
    echo "Failed job stdout log:" >> $flog
    echo "======================" >> $flog
    cat workscript.stdout >> flog
    echo "======================" >> $flog
    echo "Failed job stderr log:" >> $flog
    echo "======================" >> $flog
    cat workscript.stderr >> $flog
    echo -n "sending failure log back to output collector..."
    srmcp file:///`pwd`/$flog $output_collector/$flog || report_exec $?
    echo "sent"
    find . -maxdepth 1 -newer workscript.bash ! -type d ! -name "*x509*" -exec rm -f {} \;
    rm -rf workscript.bash
    report_exit $retcode
else
    echo "succeeded"
fi

echo -n "gathering results..."
outfiles=`find . -maxdepth 1 -type f -newer workscript.bash ! -name "*x509*"`
retcode=$?
[ $retcode = 0 ] || error_exit $retcode
tarfile=job_${1}_${2}.tar.gz
if [ -n "$outfiles" ]; then
    tar -zcf $tarfile $outfiles || error_exit $?
    rm -rf $outfiles || error_exit $?
else
    echo "no output files found!"
    error_exit 99
fi
echo "succeeded"
echo -n "sending results tarball back to output collector..."
srmcp file:///`pwd`/$tarfile $output_collector/$tarfile || error_exit $?
echo "succeeded"
echo -n "cleaning up..."
find . -maxdepth 1 -newer workscript.bash ! -type d ! -name "*x509*" -exec rm -f {} \;
rm -rf workscript.bash
echo "finished"
report_exit 0
