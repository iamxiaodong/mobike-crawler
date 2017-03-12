#!/bin/bash

mkdir db
ulimit -n 65535
ARRAY=`cat city_name`
while true; do
    echo "Started"
    for i in $ARRAY
    do
        ps -fe|grep $i |grep -v grep
        if [ $? -ne 0 ]
        then 
            python3 crawler.py $i &
        fi
    done

done
