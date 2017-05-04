#!/usr/bin/env bash

echo "Stopping AlertingServer cluster"
ps -ef | grep AlertingServer | grep java | while read -r line
do
    echo $line | awk '{print "kill " $2}' | bash
done
