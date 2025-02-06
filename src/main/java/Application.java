
import org.apache.kafka.clients.producer.KafkaProducer;  
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;


public class Application {
    public static final int min_latitude = -90;
    public static final int max_latitude = 90;
    
    public static final int min_longitude = -180;
    public static final int max_longitude = 180;


    public static void main(String argc[]){
        String bootstrapServers = "localhost:9092";
        String topic = "location-update";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> prod = new KafkaProducer<>(props);
        Random rand = new Random();
        double currentLat = 0.0;
        double currentLon = 0.0;
          try {
              while (true) {
                // double latitude  = min_latitude + (max_latitude - min_latitude + 1) * rand.nextDouble();
                // double longitude = min_longitude + (max_longitude - min_longitude + 1) * rand.nextDouble();
                double deltaLat = (rand.nextDouble() - 0.5) * 0.02;
                double deltaLon = (rand.nextDouble() - 0.5) * 0.02;
                currentLat += deltaLat;
                currentLon += deltaLon;
                String location = String.format("%.6f,%.6f",currentLat, currentLon);

                prod.send(new ProducerRecord<String, String>(topic, location));
                System.out.println("Location coordinates sent: " + location);
                Thread.sleep(1000); // Wait 1 second before sending the next message

              }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            prod.close();
        }
    }
}
