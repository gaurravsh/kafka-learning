package com.gshar.kafka.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoWithCallback {
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
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic","v3");

        // send record
        producer.send(record, (metadata,exception)-> {
            if(exception==null) {
                System.out.println("Topic :\t" + metadata.topic());
                System.out.println("Partition :\t" + metadata.partition());
                System.out.println("Offset :\t" + metadata.offset());
                System.out.println("Timestamp :\t" + metadata.timestamp());
            }
            else{
                System.out.printf("The exception : %s has occurred.%n",exception.toString());
            }
        });

        // flush and close
        producer.close();

    }
}
