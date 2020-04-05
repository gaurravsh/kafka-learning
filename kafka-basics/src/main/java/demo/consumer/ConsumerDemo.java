package demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"console-consumer-72733");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer
        consumer.subscribe(Collections.singleton("first-topic"));

        // poll
        while(true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : consumerRecords){
                System.out.println("Key :\t"+record.key());
                System.out.println("Value :\t"+record.value());
                System.out.println("Topic :\t"+record.topic());
                System.out.println("Partition :\t"+record.partition());
                System.out.println("Offset :\t"+record.offset());
                System.out.println("---------------------------------");
            }
        }

    }
}
