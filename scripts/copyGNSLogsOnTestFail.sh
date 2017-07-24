#!/bin/bash
# This script should be called from top-level GNS directory, which has build.xml file.

dataDir="/home/GNSDir"

echo $dataDir
cd $dataDir/GNS
echo "pwd "
pwd


# location where logs will be stored
logDir=$dataDir"/failedLogs"
# Some sort of Id that will be used to idetify and number logs.
failureId=$1

# making the directory if it doesn't exist.
mkdir $logDir

#mkaing new directory to store current logs
mkdir $logDir/logs-$failureId

cp -rf logs/* $logDir/logs-$failureId/.

echo "Copy of logs complete on "$HOSTNAME" failureId="$failureId

