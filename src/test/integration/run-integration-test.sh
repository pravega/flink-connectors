#!/bin/bash
#
# Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
 
# Download and install flink
set -veux
FLINK_VERSION=${FLINK_VERSION:-1.4.2}
PRAVEGA_VERSION_PREFIX=${PRAVEGA_VERSION_PREFIX:-0.3.0}
FLINK_PORTAL_PORT=${FLINK_PORTAL_PORT:-8081}
PRAVEGA_REST_PORT=${PRAVEGA_REST_PORT:-9091}
PRAVEGA_CONTROLLER_PORT=${PRAVEGA_CONTROLLER_PORT:-9090}
SCALA_VERSION=${SCALA_VERSION:-2.11}
WAIT_RETRIES=${WAIT_RETRIES:-48}
WAIT_SLEEP=${WAIT_SLEEP:-5}
HTTP_OK=200

WORK_DIR=$PWD
FLINK_DIR=$HOME/flink/flink-${FLINK_VERSION}
FLINK_BINARY=flink-${FLINK_VERSION}-bin-hadoop28-scala_${SCALA_VERSION}.tgz

trap cleanup EXIT
trap "exit 1" SIGTERM SIGHUP SIGINT SIGQUIT
cleanup() {
    # clean up flink connector artifacts as $HOME/.m2 may be cacached.
    #ls -l $HOME/.m2/repository/io/pravega/pravega-connectors-flink_2.11/
    rm -rf $HOME/.m2/repository/io/pravega/pravega-connectors-flink_2.11/*

    # clean up flink logs
    #ls -l $FLINK_DIR/log/*
    rm -f $FLINK_DIR/log/*
}

wait_for_service() {
    url=$1
    count=0
    until [ "$(curl -s -o /dev/null -w ''%{http_code}'' $url)" = "$HTTP_OK" ]; do
        if [ $count -ge ${WAIT_RETRIES} ]; then
            exit 1;
        fi
        count=$(($count+1))
        sleep ${WAIT_SLEEP}
    done
}

# clean up flink log directory that hosts job execution results
rm -f $FLINK_DIR/log/*

# Download flink
cd $HOME/flink
wget --no-check-certificate https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_BINARY}
tar zxvf $FLINK_BINARY
rm -f ${FLINK_BINARY}*

# Increase job slots, then start flink cluster
sed -i '/taskmanager.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: 5' ${FLINK_DIR}/conf/flink-conf.yaml
${FLINK_DIR}/bin/start-cluster.sh 

# wait for Flink cluster to start
wait_for_service http://localhost:${FLINK_PORTAL_PORT}

# Download and install Pravega
# For the time being, use the Pravega submodule in Flink connector.
# Eventually use a stable Pravega build that is compatible with Flink connector
cd ${WORK_DIR}/pravega
#./gradlew startstandalone 2>&1 | tee /tmp/pravega.log &
./gradlew startstandalone > /dev/null 2>&1 &

# wait for Pravega to start
wait_for_service http://localhost:${PRAVEGA_REST_PORT}/v1/scopes

cd ${WORK_DIR}
# flink connector artifact version
commit_id=$(git log --format="%h" -n 1)
count=$(git rev-list --count HEAD)
version=${PRAVEGA_VERSION_PREFIX}-${count}.${commit_id}-SNAPSHOT


# Compile and run sample Flink application
cd ${WORK_DIR}
git clone https://github.com//pravega/pravega-samples
cd ${WORK_DIR}/pravega-samples
git checkout develop
sed -i '/connectorVersion/c\connectorVersion='${version}'' gradle.properties
./gradlew :flink-examples:installDist

# start ExactlyOnceWriter
${FLINK_DIR}/bin/flink run -c io.pravega.examples.flink.primer.process.ExactlyOnceWriter flink-examples/build/install/pravega-flink-examples/lib/pravega-flink-examples-0.3.0-SNAPSHOT-all.jar --controller tcp://localhost:${PRAVEGA_CONTROLLER_PORT} --scope myscope --stream mystream --exactlyonce true

# start ExactlyOnceChecker
${FLINK_DIR}/bin/flink run -d -c io.pravega.examples.flink.primer.process.ExactlyOnceChecker flink-examples/build/install/pravega-flink-examples/lib/pravega-flink-examples-0.3.0-SNAPSHOT-all.jar --controller tcp://localhost:${PRAVEGA_CONTROLLER_PORT} --scope myscope --stream mystream 

# wait for 5 second to for job to register
sleep 2

${FLINK_DIR}/bin/flink list
job_id=`${FLINK_DIR}/bin/flink list | grep ExactlyOnceChecker | awk '{print $4}'`
count=0
ls -l ${FLINK_DIR}/log
until grep -q "EXACTLY_ONCE" ${FLINK_DIR}/log/*.out; do
    if [ $count -ge 24 ]; then
        ${FLINK_DIR}/bin/flink cancel $job_id
        exit 1
    fi
    count=$(($count+1))
    sleep ${WAIT_SLEEP}
done

# successful. stop the checker job.
${FLINK_DIR}/bin/flink stop $job_id
