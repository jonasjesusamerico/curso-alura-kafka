package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties());

        String value = "12121,23232,34343434";
        var record = new ProducerRecord<>("JONAS", value, value);

        // Producer permite passar uma função de callback para conseguir criar validações de notificação
        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso, enviando no topico:" + data.topic() + "/ partition: " + data.partition() + " / offset: " + data.offset() + " / timeStamp " + data.timestamp());
        }).get();

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
