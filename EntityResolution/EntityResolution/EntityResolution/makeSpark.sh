#!/usr/bin/env bash

nltkurl=https://pypi.python.org/packages/source/n/nltk/nltk-3.2.tar.gz#md5=cec19785ffd176fa6d18ef41a29eb227
rm -rf lib
mkdir lib
cd lib
curl -o ./nltk.tar.gz $nltkurl
tar -xf nltk.tar.gz
mv -f nltk-3.2/* ./
rm -rf nltk-3.2
rm nltk.tar.gz


cp ../*py ./
# cp ../singleheap.so ./

zip -r python-lib.zip *
cd ..