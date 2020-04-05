package com.gshar.twitterhbc.producer;

import com.google.common.collect.Lists;

import com.gshar.twitterhbc.constants.AuthConstants;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public TwitterProducer(){}

    public static void main(String[] args) {
        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<>(1_000);
        List<String> terms = Lists.newArrayList("Corona","Modi");
        Client client = createTwitterClient(msgQueue,terms);
        client.connect();

        // creating kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // creating shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            System.out.println("Twitter Client is closing.");
            client.stop();
            System.out.println("Twitter Client closed.");
            System.out.println("Kafka Producer is closing.");
            kafkaProducer.close();
            System.out.println("Kafka Producer closed.");
        }));

        while(!client.isDone()){
            String msg=null;
            try {
                msg = msgQueue.poll(3 , TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                System.out.println(msg);
                ProducerRecord<String, String> record = new ProducerRecord<>("twitter-tweets",msg);
                kafkaProducer.send(record,(recordMetadata, exception)-> {
                    if(exception!=null){
                        System.out.println("Error found : "+exception);
                    }
                });
            }
        }
    }

    public static Client createTwitterClient(BlockingQueue<String> msgQueue,List<String> terms){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        // siteStream and userStream have been discontinued anyhow. Only STREAM_HOST will be available now.
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);

        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(AuthConstants.CONSUMER_KEY, AuthConstants.CONSUMER_SECRET, AuthConstants.TOKEN, AuthConstants.SECRET);

        // create client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        return builder.build();
    }

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // safe idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // adding compression and batching
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"100");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,String.valueOf(32*1024)); // batch size = 32 KB

        // buffer configs
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,String.valueOf(1*1024*1024)); // buffer = 1 MB
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,String.valueOf(10*1000)); // wait for 10 seconds before throwing error.
        return new KafkaProducer<>(properties);
    }

}
