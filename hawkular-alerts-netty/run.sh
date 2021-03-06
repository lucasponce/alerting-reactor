#!/usr/bin/env bash

if [ ! -d "target/dependency" ]; then
    mvn clean package dependency:copy-dependencies
fi

JGROUPS_BIND_ADDR="127.0.0.1"
JAVA_OPTS="$JAVA_OPTS -Xmx64m -Xms64m -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=${JGROUPS_BIND_ADDR}"
# JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,address=8787,server=y,suspend=y"

CLUSTER=$1

if [ "$CLUSTER" == "cluster" ]
then
    NODE1="-Dhawkular-alerts.port=8080 -Dhawkular-alerts.distributed=true"
    java $JAVA_OPTS $NODE1 -cp "target/*:target/dependency/*" "org.hawkular.alerts.netty.AlertingServer" > target/AlertingServer1.out 2>&1 &
    echo "NODE1 Logs under $(pwd)/target/AlertingServer1.out"

    NODE2="-Dhawkular-alerts.port=8180 -Dhawkular-alerts.distributed=true"
    java $JAVA_OPTS $NODE2 -cp "target/*:target/dependency/*" "org.hawkular.alerts.netty.AlertingServer" > target/AlertingServer2.out 2>&1 &
    echo "NODE2 Logs under $(pwd)/target/AlertingServer2.out"
else
    java $JAVA_OPTS -cp "target/*:target/dependency/*" "org.hawkular.alerts.netty.AlertingServer" > target/AlertingServer.out 2>&1 &
    echo "Logs under $(pwd)/target/AlertingServer.out"
fi


