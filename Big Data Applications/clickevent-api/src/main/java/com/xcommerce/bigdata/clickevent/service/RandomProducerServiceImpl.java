package com.xcommerce.bigdata.clickevent.service;

import com.google.gson.Gson;
import com.xcommerce.bigdata.clickevent.model.ClickRequest;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
@Qualifier("random")
public class RandomProducerServiceImpl implements ProducerService{

    @Autowired
    KafkaMessageProducer kafkaMessageProducer;

    Gson gson;
    JSONObject jsonObject;
    List<String> cities;
    List<String> clicks;
    Random random;
    long offset;
    long end;

    @PostConstruct
    public void init(){
        gson = new Gson();
        jsonObject = new JSONObject();
        random = new Random();
        cities = Arrays.asList("İstanbul", "İzmir", "Ankara", "Antalya", "Bursa", "Van", "Elazıg", "Trabzon", "Aksaray", "Nigde", "Nevsehir", "Samsun", "Sinop");
        clicks = Arrays.asList("Login", "Siparislerim", "GununFirsatlari", "YurtDisiSatis", "Kampanyalar", "CozumMerkezi", "OkulaDonus", "EgitimCanta", "EgitimKalem", "EgitimDefter");

        offset = Timestamp.valueOf("2020-07-03 02:00:00").getTime();
        end = Timestamp.valueOf("2020-07-03 23:59:00").getTime();
    }

    @Override
    public ClickRequest producer(ClickRequest request) {
        while (true) {
            long diff = end - offset + 1;
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
            int deviceId = random.nextInt(3000 - 2000) + 2000;

            request.setDeviceId(String.valueOf(deviceId));
            request.setCurrent_ts(rand.toString());
            request.setClick(clicks.get(random.nextInt(clicks.size())));
            request.setRegion(cities.get(random.nextInt(cities.size())));

            String jsonData = gson.toJson(request);

            kafkaMessageProducer.send(jsonData);
            System.out.println(jsonData);
        }
    }
}
