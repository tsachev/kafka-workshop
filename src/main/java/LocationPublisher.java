import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class LocationPublisher {

    public static void main(String[] args) {
        Random random = new Random();
        long events = 1_000_000;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        String user = "vlado";
        double latitude = 42.697708;
        double longitude = 23.321868;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (long n = 0; n < events; n++) {
                long timestamp = System.currentTimeMillis();
                if (random.nextBoolean()) {
                    latitude += random.nextDouble();
                } else {
                    longitude += random.nextDouble();
                }
                String msg = String.join(",", String.valueOf(timestamp), user, String.valueOf(latitude), String.valueOf(longitude));
                ProducerRecord<String, String> record = new ProducerRecord<>("user.location", user, msg);
                producer.send(record);
            }
        }
    }

}