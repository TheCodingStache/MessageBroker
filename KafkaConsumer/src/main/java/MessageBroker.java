import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageBroker {
    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";

    public static void main(String[] args) {
        Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        try {
            produceMessage(10, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static void produceMessage(int numOfMessages, Producer<Long, String> kafkaProducer) throws ExecutionException, InterruptedException {
        int partition = 0;
        for (int i = 0; i < numOfMessages; i++) {
            long key = i;
            String value = String.format("event %d", i);
            long timeStamp = System.currentTimeMillis();
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, partition, timeStamp, key, value);
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            System.out.println(String.format("Record with (key %s, value %s) was sent to (partition: %d, offset: %d",
                    record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset()));
        }
    }

    public static Producer<Long, String> createKafkaProducer(String bootstrapServer) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        return new KafkaProducer<Long, String>(properties);
    }
}
