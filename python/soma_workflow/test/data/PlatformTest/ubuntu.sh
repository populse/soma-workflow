#! bin/sh

FILENAME=soma-workflow-2.5.1b
TMPNAMEPATH=/tmp/platformtest
isfrompypi=1


rm -rf $TMPNAMEPATH
mkdir $TMPNAMEPATH

rsync -a -u --exclude=".svn" --exclude=".git" ../../../../../ $TMPNAMEPATH

export SWFPATH=$TMPNAMEPATH
export SOMA_WORKFLOW_EXAMPLES=$SWFPATH/python/soma_workflow/test/data/jobExamples
export SOMA_WORKFLOW_EXAMPLES_OUT=$SWFPATH/python/soma_workflow/test/out

mkdir $SOMA_WORKFLOW_EXAMPLES_OUT

cd $TMPNAMEPATH


if [ $isfrompypi -eq 1 ]
then
  wget https://pypi.python.org/packages/source/s/soma-workflow/${FILENAME}.tar.gz
  tar -xvf $FILENAME.tar.gz
  cd $FILENAME
else
  source packaging.sh
  cd pack-soma-workflow
fi

# Installation script:
sudo apt-get update
sudo apt-get install python-qt4 python-matplotlib python-paramiko pyro
sudo python setup.py install

cd $TMPNAMEPATH/python/soma_workflow/test
cat $TMPNAMEPATH/test/PlatformTest/testlocal.stdin |  python test_workflow.py
