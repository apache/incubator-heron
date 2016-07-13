#!/bin/bash
# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Generates an Eclipse project in hero
set -e
DIR=`dirname $0`

if [ ! -d $DIR/../bazel-genfiles ]; then
    echo "Error: Directory $DIR/../bazel-genfiles does not exists." 
    echo "please buid heron first"
    exit 1
fi

# generate .project file
readonly project_file=$DIR/../.project
rm -rf $project_file
cat >> $project_file <<EOH
<?xml version="1.0" encoding="UTF-8"?>
<projectDescription>
  <name>heron</name>
  <projects/>
    <buildSpec>
      <buildCommand>
         <name>org.eclipse.jdt.core.javabuilder</name>
      </buildCommand>
      <buildCommand>
        <name>org.eclipse.m2e.core.maven2Builder</name>
      </buildCommand>
    </buildSpec>
    <natures>
      <nature>org.eclipse.jdt.core.javanature</nature>
      <nature>org.eclipse.m2e.core.maven2Nature</nature>
    </natures>
</projectDescription>
EOH

# generate .classpath file
readonly classpath_file=$DIR/../.classpath
rm -rf $classpath_file
cat >> $classpath_file <<EOH
<?xml version="1.0" encoding="UTF-8"?>
<classpath>
EOH

function generate_source_dirs() {
for srcdir in $dir_list; do
    cat >> $classpath_file << EOH
  <classpathentry kind="src" output="bin/$srcdir" path="$srcdir">$
    <attributes>$
      <attribute name="optional" value="true"/>$
      <attribute name="maven.pomderived" value="true"/>$
    </attributes>$
  </classpathentry>$
EOH
done
}

dir_list=`find $DIR/../heron -path "*/src/java" | cut -d '/' -f 3-`
generate_source_dirs

dir_list=`find $DIR/../heron -path "*/test/java" | cut -d '/' -f 3-`
generate_source_dirs

dir_list=`find $DIR/../heron -path "*/src/python" | cut -d '/' -f 3-`
generate_source_dirs

dir_list=`find $DIR/../heron -path "*/tests/python" | cut -d '/' -f 3-`
generate_source_dirs


for jarfile in ` find $DIR/../bazel-genfiles/ -name \*.jar | cut -d '/' -f 3-`; do 
  cat >> $classpath_file << EOH
  <classpathentry kind="lib" path="$jarfile"/>
EOH
done


cat >> $classpath_file << 'EOF'
  <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER/org.eclipse.jdt.internal.debug.ui.launcher.StandardVMType/JavaSE-1.8">
    <attributes>
      <attribute name="maven.pomderived" value="true"/>
    </attributes>
  </classpathentry>
  <classpathentry kind="con" path="org.eclipse.m2e.MAVEN2_CLASSPATH_CONTAINER">
    <attributes>
      <attribute name="maven.pomderived" value="true"/>
    </attributes>
  </classpathentry>
  <classpathentry kind="con" path="org.eclipse.jdt.junit.JUNIT_CONTAINER/4"/>
  <classpathentry kind="output" path="target/classes"/>
</classpath>
EOF
