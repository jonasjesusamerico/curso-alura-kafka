package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    KafkaDispatcher(String name) {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        Properties properties = new Properties();

        // Indica qual o caminho que o kafka está respondendo
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Diz qual é o tipo de serializer que será reponsavel por transformar a string para bytes tanto do key como do value
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return properties;
    }

    void send(String topic, String key, CorrelationId correlationId, T payload) throws ExecutionException, InterruptedException {
        var value = new Message<>(correlationId, payload);
        var record = new ProducerRecord<>(topic, key, value);

        // Producer permite passar uma função de callback para conseguir criar validações de notificação
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso, enviando no topico:" + data.topic() + "/ partition: " + data.partition() + " / offset: " + data.offset() + " / timeStamp " + data.timestamp());
        };

        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
