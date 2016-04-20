#! bin/bash

#linkchecker removes previous runs.
pip install linkchecker
rm -f linkchecker-errors.csv && rm -f linkchecker-out.csv
linkchecker public/index.html --no-warnings -F csv 
#uses error code: on fail, write linkchecker-errors.csv for debugging
if [[ $? == 1 ]]; then cut -sd ';' -f 1,2 linkchecker-out.csv | tr ';' ' ' | awk '{ print $2 " " $1}' | sort -u >> linkchecker-errors.csv; echo "linkchecker failed - check linkchecker-errors.csv"; else echo "linkchecker passes"; fi 
rm -f linkchecker-out.csv
