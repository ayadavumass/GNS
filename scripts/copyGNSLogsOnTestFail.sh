#!/bin/bash
# This script should be called from top-level GNS directory, which has build.xml file.

# location where logs will be stored
dataDir="/home/GNSDir/failedLogs"
# Some sort of Id that will be used to idetify and number logs.
failureId=$1

# making the directory if it doesn't exist.
mkdir $dataDir

#mkaing new directory to store current logs
mkdir $dataDir/logs-$failureId

cp -rf logs/* $dataDir/logs-$failureId/.

echo "Copy of logs complete on "$HOSTNAME" failureId="$failureId

