JAR_PATH=target/stream-kafka-spark-1.0-SNAPSHOT-jar-with-dependencies.jar

mvn clean compile assembly:single

docker cp $JAR_PATH hadoop-master:/root/