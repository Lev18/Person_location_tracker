import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.ConsumerRecords;  
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;
import java.util.Collections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;


public class Consumer {
    private static final String BOOTSERVER = "localhost:9092";
    private static final String TOPIC = "location-update";
    private static final String GROUP_ID = "location-consumer-group";
    private static final Logger logger = Logger.getLogger(Consumer.class.getName());
    private static final List<double[]> all_coordinates = Collections.synchronizedList(new ArrayList<>());

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/location_db";
    private static final String DB_USER = "user";
    private static final String DB_PASS = "1234";

    public static void main(String argc[]){
           // creating location consumer in kafka
        String url  = "jdbc:postgresql://localhost:5432/location_db";
        String user = "user";
        String pass = "1234";
        
        KafkaConsumer<String, String> consumer = createConcumer();
        //track_location(consumer, connect);
        // Start location tracker in other thread(Consumer)

        Thread tracker_thread = new Thread(()-> {
            track_location(consumer);
        });
        tracker_thread.start();

        Thread report_generator = new Thread(()->generate_report());
        report_generator.start();

        try {
            tracker_thread.join();
            report_generator.join();
        } catch(InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    

    }

    private static KafkaConsumer<String, String> createConcumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private static void track_location(KafkaConsumer<String, String> cons) {
    try (Connection connect = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
        cons.subscribe(Collections.singletonList(TOPIC)); 
        try {
            while (true) {
                ConsumerRecords<String, String> records = cons.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> rec : records) {
                    String[] coordinates = rec.value().split(",");
                    double latitude = Double.parseDouble(coordinates[0]);
                    double longitude = Double.parseDouble(coordinates[1]);

                    synchronized (all_coordinates) {
                        all_coordinates.add(new double[]{latitude, longitude});
                    }

                    insert_coordinates(connect, latitude, longitude);
                    System.out.println("Received locations: " + latitude + " " + longitude);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    } catch (SQLException e) {
        System.err.println("Database error: " + e.getMessage());
    }
}

    private static void insert_coordinates(Connection conn, double latitude, double longitude) {
        String sql_query = "INSERT INTO locations (latitude, longitude) VALUES (?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql_query)) {
            stmt.setDouble(1, latitude);
            stmt.setDouble(2, longitude);
            stmt.executeUpdate();
            logger.info("Inserted location: " + latitude + ", " + longitude);
        } catch (SQLException e) {
            logger.severe("Error inserting coordinates: " + e.getMessage());
        }
    } 

    private static void generate_report() {
        try {
            while (true) {
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
        List<double[]> coord_list = new ArrayList<>(all_coordinates);

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
