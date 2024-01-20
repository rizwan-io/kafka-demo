package io.rizwan;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperativeRebalance {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am Kafka Consumer");

        final String BOOTSTRAP_SERVER = "localhost:9092";
        final String GROUP_ID = "my-third-application";
        final String TOPIC = "demo_java";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // by default partition assignment strategy uses both
        // range and cooperative (or incremental) strategy
        // to make it work only with cooperative use the below code
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");

            // wakeup will make consumer throw a wakeup exception the next time it tries to poll
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            // consume the messages - subscribe consumer to the topic
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));

            // poll for new data
            while (true) {
                logger.info("Polling");
                ConsumerRecords<String, String> records =
                        kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offsets: " + record.offset());
                }
            }
        } catch (WakeupException wakeupException) {
            logger.info("Wake up exception!");
            // we ignore this because this is an expected exception when closing a consumer
        } catch (Exception e) {
            logger.error("Unexpected exception");
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
            logger.info("Consumer is gracefully closed");
        }
    }

}
