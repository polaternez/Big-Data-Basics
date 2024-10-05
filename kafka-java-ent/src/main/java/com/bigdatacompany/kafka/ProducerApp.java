package com.bigdatacompany.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class ProducerApp {
    public static void main(String[] args) {
        Scanner read = new Scanner(System.in);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        Producer producer = new KafkaProducer<String, String>(props);

       /* ProducerRecord<String,String> rec = new ProducerRecord<String,String>("search", "shoes");
        producer.send(rec);
        producer.close();*/

        while(true){
            System.out.println("Kafka'ya gönderilecek data:");
            String key = read.nextLine();
            ProducerRecord<String, String> rec = new ProducerRecord<String,String>("search", key);
            producer.send(rec);
        }
    }
}
