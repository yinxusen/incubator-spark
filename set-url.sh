#!/bin/bash

IP=${1}
UNAME=${2:-"sen"}
PATH=${3:-"~/git-store/incubator-spark"}
GNAME=${4:-"origin"}

if [ "x${IP}" == "x" ]; then
    echo "Usage: ./this IP [user-name] [path] [repo-name]"
    exit -1;
fi

/usr/bin/git remote set-url ${GNAME} ${UNAME}@${IP}:${PATH}

echo "$?"
