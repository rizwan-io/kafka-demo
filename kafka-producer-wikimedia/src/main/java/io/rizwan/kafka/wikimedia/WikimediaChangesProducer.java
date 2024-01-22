package io.rizwan.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "localhost:9092";


        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(URI.create(url)));
        BackgroundEventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block until then
        TimeUnit.MINUTES.sleep(10);
    }
}
