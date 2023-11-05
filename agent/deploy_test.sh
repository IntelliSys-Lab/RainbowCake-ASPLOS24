#! /bin/bash

#
# ocr-image (oi)
#

cd applications/nodejs_ocr_image
# Destroy and prepare build folder.
rm -rf build
mkdir build

# Copy files to build folder.
cp -R src/* build
cd build && npm install
zip -r index.zip *

wsk -i action update oi --kind nodejs:12 --main main --memory 1024 index.zip

cd ../../../