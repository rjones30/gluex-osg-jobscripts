#!/bin/bash
#
# osgprod_work.bash - worker script to run in container on osg worker nodes
#                     for Gluex raw data production.
#
# author: richard.t.jones at uconn.edu
# version: november 16, 2020
#
# ncpus:
# cluster:
# process:
# nstarts:
# project:
# source:
# started:

# Entry here should be inside a fresh container instance

# Input rawdata file list goes here, as in
#input_eviofile_list="root://xrootd.server.dns/path/to/file.evio ..."

# Output filename goes here, as in
#output_filename="file.evio"

function staging() {
    n=0
    for remote_input in $input_eviofile_list; do
        echo "staging loop: fetching $remote_input"
        cat $remote_input >staging.evio
        echo -n "staging loop: waiting..."
        [ $n -gt 0 ] && cat /dev/null >waitout
        echo "injecting into input sequence"
        rm -f $output_filename
        mv staging.evio $output_filename
        echo -n "staging loop: waiting..."
        cat /dev/null >waitin
        echo "block processing started"
        n=`expr $n + 1`
    done   
    echo -n "staging loop: waiting..."
    cat /dev/null >waitout
    echo "finished!"
    rm -f $output_filename
    rm -f waitin waitout
}

function safe_exit() {
    rm -f waitin waitout
    [ -n "$readloop_pid" ] && kill -SIGTERM -$readloop_pid
    exit $1
}

n=0
local_input_list=""
for rawdata in $input_eviofile_list; do
    local_input_list="$local_input_list waitin $output_filename waitout"
    n=`expr $n + 1`
done   
mkfifo waitin
mkfifo waitout

set -m
staging & readloop_pid=$!

BATCH_MODE=1
hd_root="hd_root --config=hd_recon.config \
                 -PJANA:BATCH_MODE=$BATCH_MODE \
                 -PNTHREADS=$NTHREADS \
                 -PTHREAD_TIMEOUT_FIRST_EVENT=3600 \
                 -PTHREAD_TIMEOUT=600 \
                 -p --nthreads=$NTHREADS"
$hd_root $local_input_list || safe_exit $?

rm -rf $local_input_list
touch hd_recon.config
safe_exit 0
