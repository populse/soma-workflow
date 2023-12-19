#!/bin/sh

PACKNAME=pack-soma-workflow
FULLVERSION=3.2.2
SHORTVERSION=3.2

# Remove old version
rm -rf "./$PACKNAME"

# Create new package
mkdir ./$PACKNAME
mkdir ./$PACKNAME/bin
mkdir ./$PACKNAME/python

# Copy files into package
rsync -a -u --exclude=".svn" --exclude=".git" ./python ./$PACKNAME
rsync -a -u --exclude=".svn" --exclude=".git" ./bin ./$PACKNAME

# Remove rubbish files
find -name "*.pyc" | xargs rm > /dev/null 2>&1
find -name "*.pyo" | xargs rm > /dev/null 2>&1

# Copy additional information
cp README.markdown ./$PACKNAME
cp MANIFEST.in ./$PACKNAME
cp LICENSE ./$PACKNAME

# Set version number
cat setup.py | sed -e "s/development version/$FULLVERSION/" > ./$PACKNAME/setup.py
VERSIONPATH=./$PACKNAME/python/soma_workflow
echo fullVersion = \'$FULLVERSION\' > $VERSIONPATH/version.py
echo shortVersion = \'$SHORTVERSION\' >> $VERSIONPATH/version.py
