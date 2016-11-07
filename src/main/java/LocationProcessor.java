import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Point;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.spatial4j.core.distance.DistanceUtils.DEG_TO_KM;

public class LocationProcessor {

    private final Properties consumerProperties;
    private final Properties producerProperties;
    private volatile boolean running;
    private Thread thread;

    public LocationProcessor(Properties consumerProperties, Properties producerProperties) {
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
    }


    public static void main(String[] args) {

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "spy");

        Properties producerProperties = new Properties();
        producerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        LocationProcessor locationProcessor = new LocationProcessor(consumerProperties, producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> locationProcessor.stop()));

        locationProcessor.start();
    }
    private final Point initial = SpatialContext.GEO.makePoint(42.697708, 23.321868);
    private double maxDistance = 200;

    private final Map<String, String> states = new ConcurrentHashMap<>();


    public void stop() {
        running = false;

        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        running = true;
        thread = new Thread(this::run);
        thread.start();
    }


    private void run() {
        DistanceCalculator distanceCalculator = SpatialContext.GEO.getDistCalc();

        Deserializer<String> deserializer = new StringDeserializer();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, deserializer, deserializer)) {
            consumer.subscribe(Collections.singleton("user.location"));

            Serializer<String> serializer = new StringSerializer();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, serializer, serializer)) {
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    records.forEach((record) -> {
                        String[] strings = record.value().split("[,]");
                        String user = strings[1];
                        double latitude = Double.parseDouble(strings[2]);
                        double longtitude = Double.parseDouble(strings[3]);
                        double distance = distanceCalculator.distance(initial, latitude, longtitude) * DEG_TO_KM;

                        String state = "in";
                        if (distance > maxDistance) {
                            state = "out";
                        }

                        String old = states.put(user, state);
                        if (!Objects.equals(state, old)) {
                            producer.send(new ProducerRecord<>("user.notification", user, user + " is " + state + " at " + strings[0]));
                        }
                    });
                }
            }
        }
    }


}