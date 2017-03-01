#!/bin/bash 
set -e
HOME_PATH=`dirname $0`
HOME_PATH=`cd $HOME_PATH/.. && pwd`
LIB_PATH=$HOME_PATH/lib
BIN_PATH=$HOME_PATH/bin
LOG_PATH=$HOME_PATH/logs

print_help(){
    echo "  Usage: hugegraph option"
    echo "  option value: server/notebook"
    echo 
}

java_env_check() {
   which java >/dev/null 2>&1
   if [ $? -ne 0 ];then
       echo "cannot find java in enviroment"
       exit 1
   fi
}

if [ $# -lt 1 ];then
    print_help
    exit 1
fi

unset jar
class_path="."
for jar in `ls $LIB_PATH/*.jar`;do
    class_path=$class_path:$jar
done
conf_path=$HOME_PATH/conf
class_path=$conf_path:$class_path

[ ! -d $LOG_PATH ] && mkdir $LOG_PATH

unset cmd
unset mainClass
unset java_opts
java_opts="-XX:+UseParNewGC \
           -XX:+UseConcMarkSweepGC \
           -XX:CMSInitiatingOccupancyFraction=65 \
           -XX:+CMSParallelRemarkEnabled \
           -XX:+UseCMSCompactAtFullCollection \
           -XX:CMSMaxAbortablePrecleanTime=1000 \
           -XX:+CMSClassUnloadingEnabled \
           -XX:+DisableExplicitGC \
           -Xss256k"
while [ $# -gt 0 ];do
    if [ $1 == "notebook" ];then
        cmd=$1
        mainClass="com.baidu.hugegraph.Notebook"
        shift
    elif [ $1 == "--debug" ];then
        java_opts="$java_opts \
                    -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=y"
        shift
    else
	    break;
    fi
done
java_env_check

if [ -e $BIN_PATH/${cmd}.pid ];then
    echo "$BIN_PATH/${cmd}.pid exists, there may run a $cmd now, please stop it first!"
    exit
fi
java $java_opts -cp $class_path $mainClass "$@" &
if [ $cmd == "command" -o $cmd == "client" ];then
    exit
fi

echo $! > $BIN_PATH/${cmd}.pid
