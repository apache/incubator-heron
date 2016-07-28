## How to setup eclipse dev env for twitter heron

* build twitter heron project first

```
nohup bazel build --verbose_failures  --config=ubuntu heron/...  > /tmp/result.log &
```
* generate .project and .classpath files for Eclipse IDE

```
cd scripts/
./setup-eclipse.sh
```
* import project and config Eclipse IDE 

```
JRE System Library[JavaSE-1.8] 
--> Build Path 
--> Configure Build Path 
--> Libraries 
--> Access rules
--> Edit 
--> Resloution: Accessible Rule Pattern: com/sun/net/httpserver/**
--> Resloution: Accessible Rule Pattern: com/sunmanagement/**
```
