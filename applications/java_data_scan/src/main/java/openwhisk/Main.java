package openwhisk;

import com.google.gson.*;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
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
            ScanParams scanParams = new ScanParams().count(100);
            String cur = ScanParams.SCAN_POINTER_START; 
            boolean cycleIsFinished = false;
            while(!cycleIsFinished) {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(name, cur, scanParams);
                List<Map.Entry<String, String>> scanList = scanResult.getResult();
                for (int i=0; i<scanList.size(); i++) {
                    Map.Entry<String, String> entry = scanList.get(i);
                    result.put(entry.getKey(), entry.getValue());
                }
                cur = scanResult.getCursor();
                if (cur.equals("0")) {
                    cycleIsFinished = true;
                }                 
            }
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
