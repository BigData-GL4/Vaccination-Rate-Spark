package spark.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class MeaslesDataConsumer {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        CassandraWriter cassandraWriter = new CassandraWriter();
        // Kafka consumer configuration
        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaWordCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }
        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        // Spark session
        SparkSession spark = SparkSession.builder()
                .appName("MeaslesDataConsumer")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        // Parse the Kafka value and extract the city and enrollment
        Dataset<Row> parsedDF = df.selectExpr("CAST(value AS STRING)")
                .selectExpr("split(value, ',') as row")
                .filter("row[1] IS NOT NULL AND row[8] IS NOT NULL AND row[1] != '' AND row[8] != ''")
                .selectExpr("row[1] as state", "cast(row[8] as int) as enroll");

        // Aggregate enrollments by state
        Dataset<Row> resultDF = parsedDF.groupBy("state").sum("enroll").withColumnRenamed("sum(enroll)", "total_enrollments");

//        resultDF.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();
        // Write the aggregated data to Cassandra
        resultDF.writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.foreach(row -> {
                        String state = row.getString(0);
                        long enrollments = row.getLong(1);
                        cassandraWriter.writeVaccinationData(state, (int) enrollments);
                    });
                })
                .start().awaitTermination();
    }
}
