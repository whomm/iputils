JAVA_HOME=/home/hadoop/java
HIVE_HOME=`pwd`
HADOOP_HOME=./lib
export CLASSPATH=.:$CLASSPATH:$JAVA_HOME/jre/lib/rt.jar:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$(ls $HIVE_HOME/lib/hive-serde-*.jar):$(ls $HIVE_HOME/lib/hive-exec-*.jar):$(ls $HADOOP_HOME/hadoop-core-*.jar)
echo $CLASSPATH
javac fun/*.java
java fun.Ip2Long class
java fun.Ip2Loc class
jar cvf iputils.jar ./fun


#test
#hive -e " add jar iputils.jar; create temporary function Ip2Long as 'fun.Ip2Long'; select Ip2Long('192.168.1.1') from tmp.mw;"
