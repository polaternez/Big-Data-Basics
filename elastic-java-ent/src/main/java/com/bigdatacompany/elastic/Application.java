package com.bigdatacompany.elastic;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Application {
    public static void main(String[] args) throws UnknownHostException {


        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

      /*  List<DiscoveryNode> discoveryNodes = client.listedNodes();
        for (DiscoveryNode node:discoveryNodes){
            System.out.println(node);
        }*/

//      --Index API--
      /*  Map<String, Object> json = new HashMap<>();
        json.put("name","Apple Macbook Air");
        json.put("detail","intel core i5, 16GB Ram, 128GB SSD");
        json.put("price","5400");
        json.put("provider","Apple Turkiye");

        IndexResponse indexResponse = client.prepareIndex("tech_product","_doc","3")
                .setSource(json, XContentType.JSON)
                .get();

        System.out.println(indexResponse.getId());
*/
//      --Get API--
  /*    GetResponse response = client.prepareGet("tech_product", "_doc", "3").get();

        Map<String, Object> source = response.getSource();

        String name = (String)source.get("name");
        String detail = (String)source.get("detail");
        String price = (String)source.get("price");
        String provider = (String)source.get("provider");


        System.out.println("name: " + name);
        System.out.println("detail: " + detail);
        System.out.println("price: " + price);
        System.out.println("provider: " + provider);*/

//      --Search API--

     /*   SearchResponse response = client.prepareSearch("tech_product")
                .setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("detail", "Intel"))
                .get();

        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits){
            Map<String, Object> source = hit.getSourceAsMap();
            System.out.println(source);
        }*/

        //      --Delete API--

       /* DeleteResponse response = client.prepareDelete("tech_product", "_doc", "3").get();

        System.out.println(response.getId());*/

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("name", "Apple"))
                .source("product")
                .get();
        System.out.println(response.getDeleted());

    }
}
