package com.jiang.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        //config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        //commit delay
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.jiang.producer.CustomerProducer");


        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)), (metadata, exception) -> {
                if(metadata!=null){
                    System.err.println(metadata.partition()+"---"+ metadata.offset());
                }
            });
        producer.close();
    }
}