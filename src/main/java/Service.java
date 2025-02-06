
import org.apache.kafka.clients.producer.KafkaProducer;  
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.ConsumerRecords;  
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.*;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class Service {
   
    private static final String BOOTSERVER = "localhost:9092";
    private static final String TOPIC = "location-update";
    private static final String GROUP_ID = "location-consumer-group";
    private static final Logger logger = Logger.getLogger(Service.class.getName());

    private static final int min_latitude = -90;
    private static final int max_latitude = 90;
 
    private static final int min_longitude = -180;
    private static final int max_longitude = 180;

    private static final CopyOnWriteArrayList<double[]> all_coordinates = new CopyOnWriteArrayList<>(); // Use CopyOnWriteArrayList
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String args[]) {
        // creating location producer in kafka
        KafkaProducer<String, String> producer = createProducer();

        // creating location consumer in kafka
        KafkaConsumer<String, String> consumer = createConcumer();

        // Start location simulation in separate thread(Producer)
        Thread simulator_thread = new Thread(()->simulate_location(producer));
        simulator_thread.start();

        // Start location tracker in other thread(Consumer)
        Thread tracker_thread = new Thread(()->track_location(consumer));
        tracker_thread.start();

        Thread report_generator = new Thread(()->generate_report());
        report_generator.start();

        try {
            simulator_thread.join();
            tracker_thread.join();
            report_generator.join();
        } catch(InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSERVER);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);

    }

    private static KafkaConsumer<String, String> createConcumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        return new KafkaConsumer<>(props);
    }

    private static void simulate_location(KafkaProducer<String, String> prod) {
        Random rand = new Random();
          try {
              while (running.get()) {
                double latitude  = min_latitude + (max_latitude - min_latitude) * rand.nextDouble();
                double longitude = min_longitude + (max_longitude - min_longitude) * rand.nextDouble();

                String location = String.format("%.6f,%.6f",latitude, longitude);

                prod.send(new ProducerRecord<String, String>(TOPIC, location));
                prod.flush();
                System.out.println("Sent location: " + location);
                Thread.sleep(1000); // Wait 1 second before sending the next message

              }
        } catch (Exception e) {
            e.printStackTrace();
        }
    } 

    private static void track_location(KafkaConsumer<String, String> cons) {
        cons.subscribe(Collections.singletonList(TOPIC)); 
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = cons.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        String[] coordinates = rec.value().split(",");
                        double latitude  = Double.parseDouble(coordinates[0]);
                        double longitude = Double.parseDouble(coordinates[1]);
                        all_coordinates.add(new double[]{latitude, longitude});
                            System.out.println("Received locations: " + latitude + " " + longitude);
                        //Thread.sleep(500);
                    } catch (Exception e) {
                          logger.severe("Error processing message: " + e.getMessage());
                          break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void generate_report() {
        try {
            while (running.get()) {
                Thread.sleep(5000);
                double total_distance = calculate_distance();
                logger.info("Total distance treveled: " + total_distance + "km");
            }
        } catch(InterruptedException e) {
            logger.severe("Report generation interrupted: " + e.getMessage());
        }
    }

    private static double calculate_distance() {
        double total_distance = 0.0;
        List<double[]> coord_list = all_coordinates;

        for (int i = 1; i < coord_list.size(); ++i) {
            double[] prev_coord = coord_list.get(i - 1);
            double[] curr_coord = coord_list.get(i);
            total_distance += haversine(prev_coord[0], prev_coord[1], 
                                            curr_coord[0], curr_coord[1]);
        }
        return total_distance;
    }

    private static double haversine(double prev_coord_lat, double prev_coord_lon, 
                                        double curr_coord_lat, double curr_coord_lon) {
        final int EARTH_RAD = 6371;
        double dlat = Math.toRadians(curr_coord_lat - prev_coord_lat);
        double dlon = Math.toRadians(curr_coord_lon - prev_coord_lon);

        double a = Math.pow(Math.sin(dlat/2), 2) + Math.pow(Math.sin(dlon/2),2) 
                            * Math.cos(Math.toRadians(prev_coord_lat)) 
                            * Math.cos(Math.toRadians(curr_coord_lat));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return c * EARTH_RAD;
    }
}
