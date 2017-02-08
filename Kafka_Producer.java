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
                    Class.forName("com.mysql.jdbc.Driver");
                    Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka_db","root","");
                    props.load(new FileInputStream("C://Kafka_Producer/config_producer.properties"));  
                    Statement stmt= conn.createStatement();
                    ResultSet rs= stmt.executeQuery("select * from kafka_test_tb");
                    
                    String FILENAME=props.getProperty("FILENAME"); 
                    String TOPIC=props.getProperty("TOPIC");  
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
                       
                        
                        String d="";
			while (rs.next()) 
                        {
                             
                                 producer.send(new ProducerRecord<String, String>(TOPIC,rs.getString(1))); // This publishes message on given topic
                                
                                System.out.println("--> Message [" + rs.getString(1) + "] sent.Check message on Consumer's program console");
                                d=rs.getString(1);
				
			}
                        System.out.println(d);
                        String sql = "delete from kafka_test_tb";
                                stmt=conn.prepareStatement(sql);
                                stmt.execute(sql);

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
