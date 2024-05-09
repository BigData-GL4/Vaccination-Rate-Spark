# Vaccination-Rate-Spark
This project implements a Spark Streaming application to process real-time Measles vaccination rate data. It configures the Spark processing logic within the MeaslesDataConsumer.java file. The application consumes data from Kafka, calculates total enrollments by state, and persists the results to a Cassandra database named "city_enroll" in the "spark" keyspace. Finally, a JAR file containing the application code and its dependencies is built and deployed to the Hadoop master node.

## Technologies Used
Apache Spark: Real-time data processing framework.
Apache Kafka: Distributed streaming platform.
Cassandra: NoSQL database for high-availability storage.

The provided build.sh script automates building the JAR and copying it to the Hadoop master node. Simply run:
./build.sh
