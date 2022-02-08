package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class FraudDetectorService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Collections.singletonList("JONAS"));

        while (true) {
            var poll = consumer.poll(Duration.ofMillis(100));
            if (!poll.isEmpty()) {
                System.out.println("Encontrei " + poll.count() + " registros");
                for (var record : poll) {
                    System.out.println("---------------------------------------");
                    System.out.println("Processing new order, cheking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Order processed");
                }
            }

        }

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return properties;
    }

}
