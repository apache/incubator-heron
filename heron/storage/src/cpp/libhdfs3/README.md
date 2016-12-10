libhdfs3
========================

**A Native C/C++ HDFS Client**

## Description

The Hadoop Distributed File System (HDFS) is a distributed file system designed to run on commodity hardware. HDFS is highly fault-tolerant and is designed to be deployed on low-cost hardware. HDFS provides high throughput access to application data and is suitable for applications that have large data sets.

HDFS is implemented in JAVA language and additionally provides a JNI based C language library *libhdfs*. To use libhdfs, users must deploy the HDFS jars on every machine. This adds operational complexity for non-Java clients that just want to integrate with HDFS.

**Libhdfs3**, designed as an alternative implementation of libhdfs, is implemented based on native Hadoop RPC protocol and HDFS data transfer protocol. It gets rid of the drawbacks of JNI, and it has a lightweight, small memory footprint code base. In addition, it is easy to use and deploy.

========================
## Installation

### Requirement

To build libhdfs3, the following libraries are needed.

    cmake (2.8+)                    http://www.cmake.org/
    boost (tested on 1.53+)         http://www.boost.org/
    google protobuf                 http://code.google.com/p/protobuf/
    libxml2                         http://www.xmlsoft.org/
    kerberos                        http://web.mit.edu/kerberos/
    libuuid                         http://sourceforge.net/projects/libuuid/
    libgsasl                        http://www.gnu.org/software/gsasl/

To run tests, the following libraries are needed.

    gtest (tested on 1.7.0)         already integrated in the source code
    gmock (tested on 1.7.0)         already integrated in the source code

To run code coverage test, the following tools are needed.

    gcov (included in gcc distribution)
    lcov (tested on 1.9)            http://ltp.sourceforge.net/coverage/lcov.php

### Configuration

Assume libhdfs3 home directory is LIBHDFS3_HOME.

    cd LIBHDFS3_HOME
    mkdir build
    cd build
    ../bootstrap

Environment variable CC and CXX can be used to setup the compiler.
Script "bootstrap" is basically a wrapper of cmake command, user can use cmake directly to tune the configuration. 

Run command "../bootstrap --help" for more configuration. 

### Build

Run command to build
    
    make
    
To build concurrently, rum make with -j option.

    make -j8

### Test

To do unit test, run command

    make unittest
    
To do function test, first start HDFS, and create the function test configure file at LIBHDFS3_HOME/test/data/function-test.xml, an example can be found at LIBHDFS3_HOME/test/data/function-test.xml.example. And run command.

    make functiontest
    
To show code coverage result, run command. Code coverage result can be found at BUILD_DIR/CodeCoverageReport/index.html

    make ShowCoverage

### Install

To install libhdfs3, run command

    make install
