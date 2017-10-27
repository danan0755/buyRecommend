#yarn客户端模式运行
 spark-submit --class  yks.com.scala.RecommMain  --master yarn-client   --executor-memory 4G --executor-cores 3 --driver-memory 10G --conf spark.default.parallelism=4  --jars /usr/hdp/2.4.3.0-227/spark/lib/mysql-connector-java-5.1.41-bin.jar  /mytest/recomm.jar 

#yarn集群模式运行

 spark-submit --class  yks.com.scala.RecommMain  --master yarn-cluster  --executor-memory 4G --executor-cores 3 --driver-memory 10G --conf spark.default.parallelism=4  --jars /usr/hdp/2.4.3.0-227/spark/lib/mysql-connector-java-5.1.41-bin.jar  /mytest/recomm.jar 


