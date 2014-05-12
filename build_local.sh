#!/bin/bash -e
export LOCAL_DIR=$(pwd)
export PACKAGE=foxtrot-server
export TARGET="stagech"
export INSTALL_BASE="${LOCAL_DIR}"
./make-$PACKAGE-deb
