package com.knoldus.Serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class UserDataSerializer implements Serializer {
    @Override
    public void configure(Map configs, boolean isKey){
        // no need of configuration
    }
    @Override
    public byte[] serialize(String str, Object obj)
    {
        byte [] userData= null;
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            userData = objectMapper.writeValueAsString(obj).getBytes();
        }
        catch (Exception e)
        {
            throw new SerializationException("Error while serializing the data");

        }
        return userData;
    }
    @Override
    public void close()
    {

    }
}

