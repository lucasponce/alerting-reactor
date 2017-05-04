#!/usr/bin/env bash

DIRNAME=`dirname "$0"`
$DIRNAME/stop.sh

JAVA_OPTS="$JAVA_OPTS -Xmx64m -Xms64m -Dmail.smtp.host=localhost -Dmail.smtp.port=2525"
# JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,address=8787,server=y,suspend=y"

echo "Starting AlertingServer"
java $JAVA_OPTS -cp "target/*:target/dependency/*" "org.hawkular.alerts.netty.AlertingServer" > target/AlertingServer.out 2>&1 &