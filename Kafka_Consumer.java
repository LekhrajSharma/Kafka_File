/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka_consumer;

import java.io.BufferedReader;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import static jdk.nashorn.internal.objects.NativeString.trim;

public class Kafka_Consumer {

     public static void main(String[] args) throws IOException             
    {                
        Properties props = new Properties();
        props.load(new FileInputStream("C://Kafka_Consumer/config_consumer.properties"));
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
           String FILENAME=props.getProperty("FILENAME"); 
           String TOPIC=props.getProperty("TOPIC");           
            
               consumer.subscribe(Arrays.asList(TOPIC.split("\\s*,\\s*")));    
              BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME,false));
              BufferedReader br = null;
		FileReader fr = null;
                
		try 
                {
                        
                    while (true)
                        {
                       ConsumerRecords<String, String> records = consumer.poll(100);                                                     
                            String d = "";
                            
                            for (ConsumerRecord<String, String> record : records)
                            {   
                             
                                     String c=String.valueOf(trim(record.offset()));
                                          //System.out.println(c);  
                                            d = c;
                                          // bw.write(c);
                                          // bw.newLine();                                           
                                          //System.out.print("Done");
                                //System.out.printf("offset = %d, key = %s, value = %s, topic =%s, partition=%d", record.offset(), record.key(), record.value(), record.topic(),record.partition());
                                System.out.println(record.value());
                                                      
                            }
                            //System.out.println(d);                            
                            bw.write(d);
                            bw.flush();
                        }
		} catch (IOException e) {
			e.printStackTrace();
		}         
    
    }  
    
}
