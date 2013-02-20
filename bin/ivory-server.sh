#!/bin/bash

# export JAVA_HOME=/usr/jdk64/jdk1.6.0_31

export IVORY_HOME=~/dev/data-workspace/ivory-incubating/Ivory
export IVORY_DEPLOY_HOME=~/dev/data-workspace/ivory-incubating/deploy
export IVORY_CONF_DIR=$IVORY_DEPLOY_HOME/data/conf
# export IVORY_SHARED_LIB=$IVORY_DEPLOY_HOME/data/sharedLibs
export IVORY_SHARED_LIB=$IVORY_HOME/webapp/target/ivory-webapp-0.2-SNAPSHOT/WEB-INF/lib/
export IVORY_LOG_DIR=$IVORY_DEPLOY_HOME/data/logs

SAVE_CP=$CLASSPATH

LOCAL_CLASSPATH=""
for jar in `ls $IVORY_SHARED_LIB/*.jar`;
do
   LOCAL_CLASSPATH=$LOCAL_CLASSPATH:$jar;
done;

export CLASSPATH=$IVORY_HOME/webapp/target/ivory-webapp-0.2-SNAPSHOT/WEB-INF/classes:$LOCAL_CLASSPATH:$CLASSPATH
echo $CLASSPATH

echo #########################################
echo           Starting Ivory Server ...
echo #########################################
$JAVA_HOME/bin/java -DIVORY_LOG_DIR=$IVORY_DEPLOY_HOME/data/logs -Dconfig.location=$IVORY_DEPLOY_HOME/conf -Dlog4j.debug -Dlog4j.configuration=file:$IVORY_DEPLOY_HOME/conf/log4j.xml -cp $CLASSPATH org.apache.ivory.Main $@
CLASSPATH=$SAVE_CP
