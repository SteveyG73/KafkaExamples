/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


package steveyg.kafka.examples.producer;

import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 *
 * @author sgar241
 */
public class SimpleProducer {

    /**
     * @return 
     */
    
    public static Properties getProps() {
        Properties props = new Properties();
        InputStream input = null;
        
        try {
            input = new FileInputStream("producer.properties");
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
    
    public static void main(String[] args) {
        int key;

        String topicName;
        String message;
        
        if (args.length>0) {
            topicName = args[0];
        }
        else {
            topicName = "IMSApplicationLogs";
        }
        
        Scanner userInput = new Scanner(System.in);
        Properties props = getProps();      
        
        System.out.println("Testing access to the broker...");
        
        Producer<String,String> producer = new KafkaProducer<>(props);
        System.out.print("\n Using Topic <" + topicName + ">");

        key = 0;
        
        do {
            ++key;
            System.out.print("\nMessage " + key + ": ");
            message = userInput.nextLine();
            if (!message.toUpperCase().equals("!STOP")) {
                System.out.println("Sending message "+key+" ...");
                producer.send(new ProducerRecord<>(topicName,String.valueOf(key), message));
            }
        } while (!message.toUpperCase().equals("!STOP"));
    }
    
}
