#! bin/sh

PACKNAME=pack-soma-workflow
FULLVERSION=2.5.0
SHORTVERSION=2.5


rm -rf "./$PACKNAME"

mkdir ./$PACKNAME
mkdir ./$PACKNAME/bin
mkdir ./$PACKNAME/python

rsync -a -u --exclude=".svn" ./python ./$PACKNAME
rsync -a -u --exclude=".svn" ./bin ./$PACKNAME

find -name "*.pyc" | xargs rm > /dev/null 2>&1
find -name "*.pyo" | xargs rm > /dev/null 2>&1


cp README.txt ./$PACKNAME
cp MANIFEST.in ./$PACKNAME
cp LICENSE ./$PACKNAME
cp ez_setup.py ./$PACKNAME
cp setup.py  ./$PACKNAME

VERSIONPATH=./$PACKNAME/python/soma_workflow

echo fullVersion = \'$FULLVERSION\' > $VERSIONPATH/version.py
echo shortVersion = \'$SHORTVERSION\' >> $VERSIONPATH/version.py

rm ./$PACKNAME.tar.gz
tar -zcvf ./$PACKNAME.tar.gz ./$PACKNAME 
