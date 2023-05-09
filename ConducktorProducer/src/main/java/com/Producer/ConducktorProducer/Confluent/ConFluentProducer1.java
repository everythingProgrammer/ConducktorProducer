package com.Producer.ConducktorProducer.Confluent;

import com.Producer.ConducktorProducer.Conducktor.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;

public class ConFluentProducer1 {
    private static final Logger log = LoggerFactory.getLogger(ConFluentProducer1.class.getSimpleName());

    public static void  main(String args[]){
        log.info("hello String ");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='JIUYJ2WTED6ZEEER' password='hVQ86jRJWThSRs+MXFAOOHfzE5QZxoWgep6N0+T8maCOqoQgjBEa7lmzAOPhMbx7';");
        properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());


        properties.setProperty("session.timeout.ms","45000");
        properties.setProperty("acks","all");
        //create the producer
        org.apache.kafka.clients.producer.KafkaProducer<String,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // create a producer record
        try {
            for (int j = 1; true; j++) {
                for (int i = j ; i < 1000; i++) {
                    String topic = "TopicB";
                    String key =  i+"";
//                    byte[] array = new byte[7]; // length is bounded by 7
//                    new Random().nextBytes(array);
//                    String generatedString = new String(array, Charset.forName("UTF-8"));
                    String generatedString = "This Message is with key " +i ;

                    // create a producer record
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(topic, key, generatedString);
                    //send data
                    // here with producerRecord we have an option to send a callback
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            // executed a record is sucessfully send or exception is thrown
                            if (e == null) {
                                // record was successfully sent
//                        log.info("Recieved  new metadata \n" +
//                                "Key " + key + "\n" +
//                                "Topic " + recordMetadata.topic() + "\n" +
//                                "Partition " + recordMetadata.partition() + "\n" +
//                                " Offset " + recordMetadata.offset() + "\n" +
//                                "Timestamp" + recordMetadata.timestamp() + "\n" +
//                                "" + "\n");
                                log.info("Message Is Produced " + "key " + key + " | Partition " + recordMetadata.partition());
                            } else {
                                log.error("error while producing ");
                            }
                        }
                    });
                    Thread.sleep(2000);
                }

            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
        //flush and close the producer
        // tell the producer  to send all data and block untill done - sync
        producer.flush();

        producer.close();

        }
    }

}
