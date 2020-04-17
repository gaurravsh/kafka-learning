package com.gshar.elasticsearch.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static Properties properties = new Properties();
    private static String propertiesFile = "kafka-consumer-elasticsearch/src/main/resources/ElasticConsumer.properties";

    public static void setProperties() throws IOException {
        try(InputStream inputStream = new FileInputStream(propertiesFile)){
            properties.load(inputStream);
            //System.out.println("key : "+properties.getProperty("key"));
        }
    }

    public static RestHighLevelClient getClient() throws IOException {
        setProperties();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(properties.getProperty("key"),properties.getProperty("secret")));

        RestClientBuilder builder = RestClient.builder(new HttpHost(properties.getProperty("hostname"),443,"https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        return  new RestHighLevelClient(builder);
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient restClient = getClient();
        String jsonString = "{\"chai\" : \"coffee\"}";
        IndexRequest indexRequest = new IndexRequest("twitter").source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        System.out.println("id : "+id);
        restClient.close();
    }
}
