package com.gshar.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // create properties
        Properties properties  = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        // On removing key/value serializer values, the code does not work.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer with above mentioned properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic","more message");

        // send record
        producer.send(record);

        // flush and close
        // this is commented to learn how it works
        producer.close();

    }
}
