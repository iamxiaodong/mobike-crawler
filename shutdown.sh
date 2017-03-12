#!/bin/bash

ID=`ps -ef | grep "crawler.py" | grep -v "$0" | grep -v "grep" | awk '{print $2}'`
echo $ID
echo "---------------"
for id in $ID
do
    kill $id
    echo "killed $id"
done
echo "---------------"
