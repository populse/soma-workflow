#! bin/sh

TMPNAMEPATH=/tmp/platformtest
rm -rf $TMPNAMEPATH
mkdir $TMPNAMEPATH

rsync -a -u --exclude=".svn" ../../ $TMPNAMEPATH

export SWFPATH=$TMPNAMEPATH
export SOMA_WORKFLOW_EXAMPLES=$SWFPATH/test/jobExamples
export SOMA_WORKFLOW_EXAMPLES_OUT=$SWFPATH/test/out

mkdir $SOMA_WORKFLOW_EXAMPLES_OUT

cd $TMPNAMEPATH

source packaging.sh
cd pack-soma-workflow

# Installation script:
sudo apt-get update
sudo apt-get install python-qt4 python-matplotlib python-paramiko pyro
sudo python setup.py install


cd $TMPNAMEPATH/python/soma/workflow/test
cat $TMPNAMEPATH/test/PlatformTest/testlocal.stdin |  python test_workflow.py
