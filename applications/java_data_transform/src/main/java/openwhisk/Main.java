package openwhisk;

import com.google.gson.*;
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
            str = str + ",0,0";
            records.add(str.split(","));
        }
        isReader.close();
        return records;
    }

    public static StringBuilder writeCsv(List<String[]> records) {
        StringBuilder sb = new StringBuilder();

        records.get(0)[14]="Order Processing Time";
        records.get(0)[15]="Gross Margin";
        sb.append(String.join(",", records.get(0)) +"\n");
        Set<Integer> unique_ids = new HashSet<Integer>();

        try {
            for (int i = 1; i < records.size() - 1; i++) {
                if (unique_ids.contains(Integer.parseInt(records.get(i)[6]))) {
                    continue;
                } else {
                    String val = records.get(i)[4];
                    if (val.equals("C")) {
                        records.get(i)[4] = "Critical";
                    } else if (val.equals("L")) {
                        records.get(i)[4] = "Low";
                    } else if (val.equals("M")) {
                        records.get(i)[4] = "Medium";
                    } else if (val.equals("H")) {
                        records.get(i)[4] = "High";
                    }

                    String[] date1_values = (records.get(i)[5]).split("/");
                    String[] date2_values = (records.get(i)[7]).split("/");
                    int month = Integer.parseInt(date1_values[0]);
                    int day = Integer.parseInt(date1_values[1]);
                    int year = Integer.parseInt(date1_values[2]);

                    int month2 = Integer.parseInt(date2_values[0]);
                    int day2 = Integer.parseInt(date2_values[1]);
                    int year2 = Integer.parseInt(date2_values[2]);

                    int order_time = ((year2 - year) * 365) + ((month2 - month) * 30) + (day2 - day);
                    float gross_margin = Float.parseFloat((records.get(i)[13]))
                            / Float.parseFloat((records.get(i)[11]));
                    records.get(i)[14] = (Integer.toString(order_time));
                    records.get(i)[15] = (String.valueOf(gross_margin));
                    sb.append(String.join(",", records.get(i)) + "\n");
                    unique_ids.add(Integer.parseInt(records.get(i)[6]));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't parse file " + e);
        }
        return sb;
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
        
        StringBuilder sw = null;
        try {
             List<String[]> records = readCsv(data);
            sw = writeCsv(records);
        } catch (IOException e) {
        	e.printStackTrace();
        }
       
        String fullstring=sw.toString();
        
        try {
            PrintWriter out = new PrintWriter(local_path + "processed_" + filename);
            out.println(fullstring);
        } catch (FileNotFoundException e) {
        	e.printStackTrace();
        }

        JsonObject output = new JsonObject();
        output.addProperty("result", filename + " processed!");
        
        return output;
    }
}
