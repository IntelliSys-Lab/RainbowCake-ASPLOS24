package openwhisk;

import com.google.gson.*;
import redis.clients.jedis.*;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import java.io.*;
import java.util.*;

public class Main {

    public static List<String[]> readCsv(InputStream input) throws IOException {
        // csvfile['Body'].read().decode('utf-8').split("\n") as input
        List<String[]> records = new ArrayList<String[]>();
        InputStreamReader isReader = new InputStreamReader(input);
        BufferedReader reader = new BufferedReader(isReader);

        String str;
        while ((str = reader.readLine()) != null) {
            records.add(str.split(","));
        }
        isReader.close();
        return records;
    }

    public static void writeRedis(List<String[]> records, String name) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(300);
        poolConfig.setMaxIdle(100);
        poolConfig.setMinIdle(1);
        JedisPool pool = new JedisPool(poolConfig, "172.17.0.1", 6379, 30000, "openwhisk", 0);
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Pipeline p = jedis.pipelined();

            for (int i = 1; i < records.size() - 1; i++) {
                p.hset(name, records.get(i)[6], records.get(i)[13]);
            }
            p.sync();
            jedis.close();
        } catch (Exception e) {
            throw new RuntimeException("Can't write Redis: " + e);
        }
    }

    public static JsonObject main(JsonObject args) {
        String filename = "10000_Sales_Records.csv";
        String local_path = "/";

        InputStream data = null;
        try {
            data = Main.class.getResourceAsStream(local_path + filename);
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            List<String[]> records = readCsv(data);
            writeRedis(records, filename);
        } catch (Exception e) {
        	e.printStackTrace();
        }
       
        JsonObject output = new JsonObject();
        output.addProperty("result", filename + " write to Redis!");
        
        return output;
    }
}
