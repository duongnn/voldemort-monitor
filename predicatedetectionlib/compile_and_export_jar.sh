#!/bin/bash

classpath=../../../lib/commons-codec-1.6.jar:../../../lib/guava-14.0.1.jar:../../../lib/jopt-simple-4.6.jar:.
outputdir=../../../classes


# Option 1: update to jar file only

# # compile code for common
# cd ./src/main/java
# javac -d $outputdir ./predicatedetectionlib/common/*.java -classpath $classpath
# javac -d $outputdir ./predicatedetectionlib/monitor/*.java -classpath $classpath

# # update jar file
# cd ../../../classes
# jar uvf predicatedetectionlib.jar ./predicatedetectionlib/*


# Option 2: obtain a freshly newly updated predicatedetectionlib.jar

# remove old classes files
rm -rf ./classes/*

# compile code for common
cd ./src/main/java
javac -d $outputdir ./predicatedetectionlib/versioning/*.java -classpath $classpath
javac -d $outputdir ./predicatedetectionlib/common/predicate/*.java -classpath $classpath
javac -d $outputdir ./predicatedetectionlib/common/clientgraph/*.java -classpath $classpath
javac -d $outputdir ./predicatedetectionlib/common/*.java -classpath $classpath
javac -d $outputdir ./predicatedetectionlib/monitor/*.java -classpath $classpath


# delete predicatedetectionlib.jar
# and then create it again, like below
cd ../../../classes
jar cvf predicatedetectionlib.jar ./predicatedetectionlib/*

