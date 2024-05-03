package spark.kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

public class MeaslesDataConsumer {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: MeaslesDataConsumer <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        // Set up Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Set up SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("MeaslesDataConsumer")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        // Process the received data to calculate enrollment numbers for each city
        Dataset<Row> processedData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .flatMap((FlatMapFunction<Tuple2<String, String>, String>) value -> Arrays.asList(value._2.split("\n")).iterator(), Encoders.STRING())
                .map((MapFunction<String, Tuple2<String, Double>>) line -> {
                    String[] parts = line.split(",");
                    String city = parts[5]; // Assuming city is at index 5
                    double enroll = Double.parseDouble(parts[8]); // Assuming enrollment is at index 8
                    return new Tuple2<>(city, enroll);
                }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
                .groupBy("_1")
                .sum("_2")
                .withColumnRenamed("_1", "City")
                .withColumnRenamed("sum(_2)", "Enrollment");

        // Start the streaming query
        StreamingQuery query = processedData.writeStream()
                .outputMode("update") // Change output mode according to your requirement
                .format("console")
                .start();

        query.awaitTermination();

        // Close Kafka producer
        producer.close();
    }
}
