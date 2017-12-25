#!/bin/bash
# Run using bash run_pipeline.sh IMAGEFOLDER OUTPUTFOLDER

hadoop fs -mkdir /app
hadoop fs -put -f $1 /app/$1

rm -rf /tmp/output

python pipeline.py /app/$1 /app/$2 localhost 9000

hadoop fs -get /app/$2 $2

echo "DONE"
