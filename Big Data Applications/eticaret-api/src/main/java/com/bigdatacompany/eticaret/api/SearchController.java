package com.bigdatacompany.eticaret.api;

import com.bigdatacompany.eticaret.MessageProducer;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {

    @Autowired
    MessageProducer messageProducer;

    @GetMapping("/search")
    public void searchIndex(@RequestParam String term) {

        List<String> cities = Arrays.asList("İstanbul", "İzmir", "Ankara", "Adana", "Mersin", "Zonguldak", "Malatya", "Elazıg", "Hakkari", "Trabzon", "Tekirdag");
        List<String> products = Arrays.asList("Diaper", "Phone", "Keyboard", "Mouse", "Glass", "Wallet", "Laptop", "Mop", "Bulb", "Hard Disk", "Armchair");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        Random random = new Random();
        while (true){
            int i = random.nextInt(cities.size());
            int k = random.nextInt(products.size());

            long offset = Timestamp.valueOf("2020-07-03 02:00:00").getTime();
            long end = Timestamp.valueOf("2020-07-03 23:59:00").getTime();
            long diff = end - offset + 1;
            Timestamp rand = new Timestamp(offset + (long)(Math.random()+diff));

            JSONObject jsonObject = new JSONObject();

            jsonObject.put("search", products.get(k));
            jsonObject.put("current_ts", rand.toString());
            jsonObject.put("region", cities.get(i));
            jsonObject.put("userid", random.nextInt(15000-1000) + 1000);

            System.out.println(jsonObject.toJSONString());
            messageProducer.send(jsonObject.toJSONString());
        }
    }

    @GetMapping("/search/stream")
    public void searchIndexStream(@RequestParam String term) {

        List<String> cities = Arrays.asList("İstanbul", "İzmir", "Ankara", "Adana", "Mersin", "Zonguldak", "Malatya", "Elazıg", "Hakkari", "Trabzon", "Tekirdag");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        Random random = new Random();
        int i = random.nextInt(cities.size());
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("search", term);
        jsonObject.put("current_ts", timestamp.toString());
        jsonObject.put("region", cities.get(i));
        jsonObject.put("userid", random.nextInt(15000-1000) + 1000);

//        System.out.println(jsonObject.toJSONString());
        messageProducer.send(jsonObject.toJSONString());

    }

}
