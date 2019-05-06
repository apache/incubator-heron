#!/bin/bash

ROOT_DIR=`pwd`

WORK_DIR=/website/public
ME=`basename $0`

rm -rf $WORK_DIR
mkdir -p $WORK_DIR

cd website

make setup

make site

# push all of the results to asf-site branch
git checkout asf-site
git clean -f -d
git pull origin asf-site

#
cd $ROOT_DIR

rm -rf content
mkdir content

cp -a $WORK_DIR/* content
cp -a .htaccess content
git add content
git commit -m "git-site-role commit from $ME"
git push origin asf-site
