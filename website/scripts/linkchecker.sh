#!/bin/bash

set -e

function die {
  echo $1 && exit 1
}

OUT_FILE=linkchecker-out.csv
ERRORS_FILE=linkchecker-errors.csv
FILE_TO_CHECK=public/index.html

#linkchecker removes previous runs.
which linkchecker || die 'Linkchecker must be installed to run this script. Exiting'
rm -f $OUT_FILE $ERRORS_FILE

set +e
linkchecker $FILE_TO_CHECK --no-warnings -F csv
STATUS=$?
set -e

#uses error code: on fail, write linkchecker-errors.csv for debugging
if [[ $STATUS != 0 ]]; then
  cut -sd ';' -f 1,2 $OUT_FILE | tr ';' ' ' | \
    awk '{ print $2 " " $1}' | sort -u >> $ERRORS_FILE;
  echo "linkchecker failed - check $ERRORS_FILE";
  rm -f $OUT_FILE
  exit $STATUS
else
  rm -f $OUT_FILE
  echo "linkchecker passes";
fi
