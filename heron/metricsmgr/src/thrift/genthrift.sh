rm -rf gen-javabean
rm -rf ../java/org/apache/scribe/*
thrift --gen java:beans,hashcode,nocamel,generated_annotations=undated scribe.thrift
cp -r gen-javabean/*  ../java/
rm -rf gen-javabean
