package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties());

        for (int i = 0; i < 100; i++) {


            var key = UUID.randomUUID().toString();
            String value = key + ",23232a,34343434";
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

            // Producer permite passar uma função de callback para conseguir criar validações de notificação
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Sucesso, enviando no topico:" + data.topic() + "/ partition: " + data.partition() + " / offset: " + data.offset() + " / timeStamp " + data.timestamp());
            };

            producer.send(record, callback).get();

            var email = "Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

            producer.send(emailRecord, callback).get();
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();

        // Indica qual o caminho que o kafka está respondendo
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Diz qual é o tipo de serializer que será reponsavel por transformar a string para bytes tanto do key como do value
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
