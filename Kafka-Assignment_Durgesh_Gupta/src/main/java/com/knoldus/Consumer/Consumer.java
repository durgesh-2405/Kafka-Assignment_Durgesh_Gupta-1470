package com.knoldus.Consumer;

import com.knoldus.Model.UserModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args)
    {
        ConsumerListener objcon = new ConsumerListener();
        Thread thread= new Thread(objcon);
        thread.start();
    }
    public static void consumer()
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","com.knoldus.Deserializer.UserDataDeserializer");
        properties.put("group.id","test-group");
        KafkaConsumer<String, UserModel> modelKafkaConsumer = new KafkaConsumer(properties);
        List userData = new ArrayList();
        userData.add("UserData");
        modelKafkaConsumer.subscribe(userData);
        try
        {
            while(true)
            {
                ConsumerRecords<String, UserModel> dataRecords = modelKafkaConsumer.poll(Duration.ofMillis(100));
                //if(dataRecords.isEmpty())
                //{
                    //System.out.println("No data");
                //}
                ObjectMapper objectMapper = new ObjectMapper();
                FileWriter fileWriter = new FileWriter("UserData.txt",true);
                for (ConsumerRecord<String, UserModel> record: dataRecords)
                {
                    System.out.println(record.value().getName());
                    System.out.println("Data recieved successfully");
                    System.out.println(objectMapper.writeValueAsString(record.value()));
                    fileWriter.append(objectMapper.writeValueAsString(record.value())+"\n");
                }
                fileWriter.close();
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        finally
        {
            modelKafkaConsumer.close();
            System.out.println("Consumer is closed");
        }
    }
}

class ConsumerListener implements Runnable
{
    @Override
    public void run()
    {
        Consumer.consumer();
    }
}