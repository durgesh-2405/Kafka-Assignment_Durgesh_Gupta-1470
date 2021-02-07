package com.knoldus.Deserializer;


import com.knoldus.Model.UserModel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class UserDataDeserializer implements Deserializer {
    @Override
    public void configure(Map cofigs, boolean isKey)
    {
        // no need for configuration
    }
    @Override
    public Object deserialize(String str, byte[] userData)
    {
        ObjectMapper mapper= new ObjectMapper();
        UserModel userModelData = null;
        try
        {
            userModelData = mapper.readValue(userData, UserModel.class);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new SerializationException("Error during deserialization");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return userModelData;
    }
    //@Override
    //public void close()
    //{

    //}
}
