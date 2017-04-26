#!/usr/bin/env bash

echo "Stopping AlertingServer"
ps -ef | grep java | grep AlertingServer | awk '{print "kill -15 " $2}' | bash
