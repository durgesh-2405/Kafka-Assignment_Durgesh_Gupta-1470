package com.knoldus.Producer;

import com.knoldus.Model.UserModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer
{
    public static void main(String [] args)
    {
        Properties properties= new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","com.knoldus.Serializer.UserDataSerializer");

        KafkaProducer<String, UserModel> modelKafkaProducer = new KafkaProducer<>(properties);
        try
        {
            UserModel dataOne = new UserModel(1,"Durgesh", 21, "B.tech");
            UserModel dataTwo = new UserModel(2,"Durgesh", 22, "B.Sc");
            UserModel dataThree= new UserModel(3,"Durgesh",23, "BCA");
            UserModel dataFour= new UserModel(4,"Durgesh",22,"M.Tech");
            modelKafkaProducer.send(new ProducerRecord<>("UserData","FirstVal",dataOne));
            modelKafkaProducer.send(new ProducerRecord<>("UserData","SecondVal",dataTwo));
            modelKafkaProducer.send(new ProducerRecord<>("UserData","ThirdVal",dataThree));
            modelKafkaProducer.send(new ProducerRecord<>("UserData","FourthVal",dataFour));
            System.out.println("Message produced and sent successfully");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            modelKafkaProducer.close();
        }
    }

}
