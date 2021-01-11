#!/bin/bash
export MYSQL_DATABASE="cuttle"
export MYSQL_USERNAME="cuttle"
export MYSQL_PASSWORD="cuttle"
# -DdevMode=True
#sbt  "examples / runMain com.criteo.cuttle.examples.HelloCronScheduling"

# docker run --name cuttle_db -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=cuttle -e MYSQL_USER=cuttle -e MYSQL_PASSWORD=cuttle -d -p 3306:3306  mariadb:10.2
if [ "$1" == "cron" ]; then
    sbt "examples / runMain com.criteo.cuttle.examples.HelloCronScheduling"
else
    sbt "examples / runMain com.criteo.cuttle.examples.HelloTimeSeries"
fi;