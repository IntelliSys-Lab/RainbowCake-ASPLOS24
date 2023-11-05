package openwhisk;

import com.google.gson.*;
import redis.clients.jedis.*;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import java.io.*;
import java.util.*;

public class Main {

    public static Map<String, String> readRedis(String name) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(300);
        poolConfig.setMaxIdle(100);
        poolConfig.setMinIdle(1);
        JedisPool pool = new JedisPool(poolConfig, "172.17.0.1", 6379, 30000, "openwhisk", 0);
        Jedis jedis = null;
        Map<String, String> result = null;
        try {
            jedis = pool.getResource();
            result = jedis.hgetAll(name);
        } catch (Exception e) {
            throw new RuntimeException("Can't read Redis: " + e);
        }
        return result;
    }

    public static JsonObject main(JsonObject args) {
        String filename = "10000_Sales_Records.csv";

        try {
            readRedis(filename);
        } catch (Exception e) {
        	e.printStackTrace();
        }
       
        JsonObject output = new JsonObject();
        output.addProperty("result", filename + " read from Redis!");
        
        return output;
    }
}
