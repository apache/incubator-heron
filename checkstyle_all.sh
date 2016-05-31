#!/bin/bash
#
# Run checkstyles on all java files at once. Written to be invoked via bazel:
#
#  bazel run :checkstyle_all
#

set -e

ROOT_DIR=`git rev-parse --show-toplevel`
JARS=`find $PWD/. -name "*\.jar" | tr '\n' ":"`
JAVA_FILES=`find $ROOT_DIR/{heron,tools,integration-test}/. -name "*.java"`
JAVA_FILES_COUNT=`echo $JAVA_FILES | wc -w | xargs`
CONFIG=$ROOT_DIR/tools/java/src/com/twitter/bazel/checkstyle/coding_style.xml

echo "Checkstyles checking $JAVA_FILES_COUNT java files"
start_secs=`date +%s`
(cd $ROOT_DIR && java -cp $JARS com.puppycrawl.tools.checkstyle.Main -c $CONFIG $JAVA_FILES)
end_secs=`date +%s`
duration=`expr $end_secs - $start_secs`
echo "Checkstyles successfully checked $JAVA_FILES_COUNT java files in $duration seconds"
