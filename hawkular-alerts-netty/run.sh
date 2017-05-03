#!/usr/bin/env bash

if [ ! -d "target/dependency" ]; then
    mvn clean package dependency:copy-dependencies
fi

JAVA_OPTS="$JAVA_OPTS -Xmx64m -Xms64m"
JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,address=8787,server=y,suspend=y"
java $JAVA_OPTS -cp "target/*:target/dependency/*" "org.hawkular.alerts.netty.AlertingServer"