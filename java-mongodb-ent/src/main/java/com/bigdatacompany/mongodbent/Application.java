package com.bigdatacompany.mongodbent;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {

        MongoClient client = new MongoClient("localhost",27017);
        MongoDatabase infoDB = client.getDatabase("info");
        MongoCollection<Document> personnelCollection = infoDB.getCollection("personnel");

        BasicDBObject data = new BasicDBObject()
                .append("name","Thomas Edison")
                .append("date",1847)
                .append("country","USA");

        BasicDBObject data2 = new BasicDBObject()
                .append("name", "Elon Musk")
                .append("date", 1971)
                .append("country", "Africa")
                .append("job", "entrepreneur");

//       --CREATE--

//         personnelCollection.insertOne(Document.parse(data.toJson()));

  /*      Document parse = Document.parse(data.toJson());
        Document parse2 = Document.parse(data2.toJson());

        personnelCollection.insertMany(Arrays.asList(parse, parse2));*/

//      --READ--

/*
//        FindIterable<Document> results = personnelCollection.find();
        FindIterable<Document> results = personnelCollection.find(new BasicDBObject("date", 1971));

        for (Document doc : results){
            System.out.println(doc.toJson());
        }*/

//      --UPDATE--

       /* Bson filter = Filters.exists("job");
        Bson update = Updates.set("child", "Nevada Musk");

        personnelCollection.updateOne(filter, update);*/

//      --DELETE--

  /*      Bson deleteFilter = Filters.eq("name", "Elon Musk");
        personnelCollection.deleteOne(deleteFilter);*/

       /* Bson deleteFilter = Filters.eq("country", "USA");
        personnelCollection.deleteMany(deleteFilter);*/

//        personnelCollection.drop();
//        infoDB.drop();

    }
}
