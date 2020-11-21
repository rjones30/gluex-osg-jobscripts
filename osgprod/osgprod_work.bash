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
    past_input=""
    for remote_input in $input_eviofile_list; do
        if [ $n = 0 ]; then
            local_input="$output_filename"
        else
            local_input="${output_filename}+$n"
        fi
        cat $remote_input >$local_input
        cat /dev/null >wait$n && rm wait$n
        [ -n "$past_input" ] && rm -f $past_input
        past_input=$local_input
        n=`expr $n + 1`
    done   
    cat /dev/null >wait$n && rm wait$n
    rm -f $past_input
}

n=0
for rawdata in $input_eviofile_list; do
    if [ $n = 0 ]; then
        local_input_list="wait0 $output_filename"
    else
        local_input_list="$local_input_list wait$n ${output_filename}+$n"
    fi
    mkfifo wait$n
    n=`expr $n + 1`
done   
mkfifo wait$n

staging &

BATCH_MODE=1
hd_root="hd_root --config=hd_recon.config \
                 -PJANA:BATCH_MODE=$BATCH_MODE \
                 -PNTHREADS=$NTHREADS \
                 -PTHREAD_TIMEOUT_FIRST_EVENT=3600 \
                 -PTHREAD_TIMEOUT=600 \
                 -p --nthreads=$NTHREADS"
$hd_root $local_input_list || exit $?

rm -rf $local_input_list
touch hd_recon.config
