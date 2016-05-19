#!/bin/bash

set -e

#linkchecker removes previous runs.
which linkchecker || pip install linkchecker
rm -f linkchecker-errors.csv && rm -f linkchecker-out.csv

set +e
linkchecker public/index.html --no-warnings -F csv
STATUS=$?
set -e

#uses error code: on fail, write linkchecker-errors.csv for debugging
if [[ $STATUS != 0 ]]; then
  cut -sd ';' -f 1,2 linkchecker-out.csv | tr ';' ' ' | \
  awk '{ print $2 " " $1}' | sort -u >> linkchecker-errors.csv;
  echo "linkchecker failed - check linkchecker-errors.csv";
   exit $STATUS
else
  echo "linkchecker passes"; 
fi 

rm -f linkchecker-out.csv
