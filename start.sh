#!/bin/bash

mkdir db
ulimit -n 65535
ARRAY=`cat city_name`
time=`date +%Y%m%d%T`
while true; do
    for i in $ARRAY
    do
        ps -fe|grep $i |grep -v grep
        if [ $? -ne 0 ]
        then 
            echo "$i process started at $time"
            python3 crawler.py $i &
        fi
    done
    sleep 1m
done
