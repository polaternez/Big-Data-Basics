import com.google.gson.Gson;
import model.SearchProductModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        String topic = "search";
        Scanner read = new Scanner(System.in);
        Gson gson = new Gson();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        Producer producer = new KafkaProducer<String, String>(props);

        while (true){
            System.out.println("Search:");
            String product = read.nextLine();

            SearchProductModel sp = new SearchProductModel();
            sp.setProduct(product);
            String time = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
            sp.setTime(time);

            String json = gson.toJson(sp);
            System.out.println(json);

            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, json);
            producer.send(rec);

            System.out.println("Sent to Kafka!!");

        }
    }
}
