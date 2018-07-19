#!/usr/bin/env bash

pids=`ps -ef | grep raftexample2 | awk '{print $2}'`
for pid in ${pids[@]}
do
    echo ${pid}
    kill -9 $pid
done