import netscape.javascript.JSObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class Application {
    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200)));

        IndexRequest request = new IndexRequest("product");

        File file = new File("C:\\Users\\Master\\Desktop\\Big Data\\Datasets\\Applications\\AutocompleteApp\\products.csv");
        Scanner scanner = new Scanner(file);

        while (scanner.hasNext()){
            String line = scanner.nextLine();
            String[] terms = line.split(",");
            String brand = terms[0];
            String title = terms[1];

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("brand", brand);
            jsonObject.put("title", title);

            request.source(jsonObject.toJSONString(), XContentType.JSON);

            client.index(request, RequestOptions.DEFAULT);

        }

    }
}
