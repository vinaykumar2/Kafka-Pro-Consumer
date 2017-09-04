/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vkumar1
 */
 import java.util.*;
   import kafka.consumer.Consumer;
   import kafka.consumer.ConsumerConfig;
   import kafka.consumer.ConsumerIterator;
   import kafka.consumer.KafkaStream;
   import kafka.javaapi.consumer.ConsumerConnector;

    public class KafkaConsumer {
       private ConsumerConnector consumerConnector = null;
       private final String topic = "jcsdemo0056-mytopic";

       public void initialize() {
             Properties props = new Properties();
             props.put("zookeeper.connect", "129.152.128.177:2181"); // localhost:2181 for local kafka server.
             props.put("group.id", "testgroup");
             props.put("zookeeper.session.timeout.ms", "400");
             props.put("zookeeper.sync.time.ms", "300");
             props.put("auto.commit.interval.ms", "1000");
             ConsumerConfig conConfig = new ConsumerConfig(props);
             consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
       }

       public void consume() {
             //Key = topic name, Value = No. of threads for topic
             Map<String, Integer> topicCount = new HashMap<String, Integer>();       
             topicCount.put(topic, new Integer(1));
            
             //ConsumerConnector creates the message stream for each topic
             Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                   consumerConnector.createMessageStreams(topicCount);         
            
             // Get Kafka stream for topic 'mytopic'
             List<KafkaStream<byte[], byte[]>> kStreamList =
                                                  consumerStreams.get(topic);
             // Iterate stream using ConsumerIterator
             for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
                    ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
                   
                    while (consumerIte.hasNext())
                           System.out.println("Message consumed from topic [" + topic + "] : "       +
                                           new String(consumerIte.next().message()));              
             }
             //Shutdown the consumer connector
             if (consumerConnector != null)   consumerConnector.shutdown();          
       }

       public static void main(String[] args) throws InterruptedException {
             KafkaConsumer kafkaConsumer = new KafkaConsumer();
             // Configure Kafka consumer
             kafkaConsumer.initialize();
             // Start consumption
             kafkaConsumer.consume();
       }
   }