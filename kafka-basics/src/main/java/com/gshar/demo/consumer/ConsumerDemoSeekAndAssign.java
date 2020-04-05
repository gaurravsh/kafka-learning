package com.gshar.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoSeekAndAssign {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"console-consumer-72733");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // assign and seek are mainly used to replay data and to fetch a specific message

        // assign consumer
        TopicPartition topicPartition1 = new TopicPartition("first-topic",0);
        consumer.assign(Arrays.asList(topicPartition1));

        // seek
        consumer.seek(topicPartition1,0L);

        // stop reading after 5 messages
        final int maxMsgCount=5;
        boolean keepOnReading = true;
        int messagesReadSoFar = 0;

        // poll
        while(keepOnReading){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : consumerRecords){
                System.out.println("Key :\t"+record.key());
                System.out.println("Value :\t"+record.value());
                System.out.println("Topic :\t"+record.topic());
                System.out.println("Partition :\t"+record.partition());
                System.out.println("Offset :\t"+record.offset());
                System.out.println("---------------------------------");
            }

            messagesReadSoFar += consumerRecords.count();
            if(messagesReadSoFar >  5)
                keepOnReading = false;
        }

        System.out.println("Total messages read so far : "+messagesReadSoFar);

    }
}
