package com.Producer.ConducktorProducer.Conducktor;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());

    public static void  main(String args[]){
        log.info("hello String ");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"202MPZsJlCVsWX5XqoAs3V\" password=\"0d83f270-e028-4217-b22b-5eb21cdbb674\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());


        //create the producer
        org.apache.kafka.clients.producer.KafkaProducer<String,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // create a producer record

        for(int j = 0 ; j<1 ; j++) {
            for (int i = 30; i < 100; i++) {
                String topic = "MessageQueue";
                String key = "id_" + i;
                byte[] array = new byte[7]; // length is bounded by 7
                new Random().nextBytes(array);
                String generatedString = new String(array, Charset.forName("UTF-8"));


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
                            log.info("key " + key + " | Partition " + recordMetadata.partition());
                        } else {
                            log.error("error while producing ");
                        }
                    }
                });
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //flush and close the producer
        // tell the producer  to send all data and block untill done - sync
        producer.flush();

        producer.close();
    }

}
