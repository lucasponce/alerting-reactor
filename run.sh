#!/usr/bin/env bash

if [ ! -d "target/dependency" ]; then
    mvn clean package dependency:copy-dependencies
fi

JGROUPS_BIND_ADDR="127.0.0.1"

PROPS="$(pwd)/${1}.properties"

JAVA_OPTS="$JAVA_OPTS -Xmx64m -Xms64m -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=${JGROUPS_BIND_ADDR} -Dalerting.properties=${PROPS}"
java $JAVA_OPTS -cp "target/*:target/dependency/*" "org.hawkular.alerting.reactor.AlertingServer"