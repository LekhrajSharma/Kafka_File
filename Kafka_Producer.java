/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka_producer;

import java.util.*;
import java.io.*;
import java.sql.*;
import org.apache.kafka.clients.producer.ProducerRecord;
public class Kafka_Producer {

   private static org.apache.kafka.clients.producer.Producer producer;
     Properties props = new Properties();
    public void initialize()
    {       
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }
    
    public void publishMessage() throws Exception
    {
        
          
          BufferedReader br = null;
		FileReader fr = null;
                try {
                    props.load(new FileInputStream("C://Kafka_Producer/config_producer.properties"));  
                    // get the properties and print  
                    //props.list(System.out);  
                    //Reading each property value  
                    String FILENAME=props.getProperty("FILENAME"); 
                    String TOPIC=props.getProperty("TOPIC");  
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
                        //System.out.println(br.toString()+"test");
                        
			while ((sCurrentLine = br.readLine()) != null) 
                        {
                             
                                 producer.send(new ProducerRecord<String, String>(TOPIC,sCurrentLine)); // This publishes message on given topic
                                //if("Y".equals(sCurrentLine)){ break; }
                                System.out.println("--> Message [" + sCurrentLine + "] sent.Check message on Consumer's program console");
				//System.out.println(sCurrentLine);
			}
                        PrintWriter writer = new PrintWriter(FILENAME);
                        writer.print("");
                        writer.close();
		} catch (IOException e) {                    
			e.printStackTrace();
		} finally {
			try {
                            if (br != null)
					br.close();
                            
                                    if (fr != null)
                                            fr.close();

			}   catch (IOException ex) {
				ex.printStackTrace();
                            }
		}
    }
   
    public static void main(String args[])throws Exception
    {
        Kafka_Producer kafkaproducer = new Kafka_Producer();
        kafkaproducer.initialize();
        kafkaproducer.publishMessage();
        producer.close();
    }
    
}
