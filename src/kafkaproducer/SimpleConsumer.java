/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafkaproducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 *
 * @author sgar241
 */
public class SimpleConsumer {

    
    public static Properties getProps() {
        Properties props = new Properties();
        InputStream input = null;
        
        try {
            input = ClassLoader.class.getResourceAsStream("/kafkaproducer/consumer.properties");
            props.load(input);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        return(props);
        
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String topicName = "IMSApplicationLogs";

        Properties props = getProps();    

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        System.out.println("Subscribing to topic <" + topicName + ">");
        //consumer.subscribe(Arrays.asList(topicName));
        System.out.println("Set to begining of topic");
        TopicPartition part0 = new TopicPartition(topicName,0);
        consumer.assign(Arrays.asList(part0));
        consumer.seekToBeginning(part0);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}
