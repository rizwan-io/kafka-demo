package io.rizwan;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am Kafka Producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Changing to round robin (to test)
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // send data - this is asynchronous operation
        // when we are sending all the messages together or with very small-time interval
        // then kafka uses StickyPartitioner and sends all the messages in a batch to one partition
        // this is for performance enhancement - This is the default partitioner with new version of Kafka
        // if you want to change in to round robin lets add a sleep method.
        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "Hello World: " + i);


            producer.send(producerRecord, (recordMetadata, e) -> {
                // executes everything a record is successfully sent or and exception is thrown
                if (e == null) {
                    // the record has been successfully sent
                    logger.info("Received new metadata/ \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + ";");
                } else {
                    logger.error("Error while producing:", e);
                }
            });
        }

        // flush and close the producer - flush is sync operation
        producer.flush();

        // close automatically calls flush no not required to explicitly call flush
        producer.close();
    }
}