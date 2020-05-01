import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class App {

    static void indexAPI(TransportClient client){
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("name", "Dell G315");
        data.put("detail", "Intel Core i7, 12GB Ram, 250GB SSD");
        data.put("price", "8500");
        data.put("provider", "Dell Turkiye");

        IndexResponse indexResponse = client.prepareIndex("product", "_doc", "1")
                .setSource(data, XContentType.JSON)
                .get();
    }

    static void getAPI(TransportClient client){
        GetResponse response = client.prepareGet("product", "_doc", "1").get();
        Map<String, Object> source = response.getSource();
        String name = (String) source.get("name");
        String price = (String) source.get("price");
        String detail = (String) source.get("detail");
        String provider = (String) source.get("provider");

        System.out.println("name: " + name);
        System.out.println("price: " + price);
        System.out.println("detail: " + detail);
        System.out.println("provider: " + provider);
    }

    static void searchAPI(TransportClient client){
        SearchResponse searchResponse = client.prepareSearch("product").setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("provider", "Turkiye"))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();

        for (SearchHit hit : hits ){
            Map<String, Object> sourceMap = hit.getSourceAsMap();
            System.out.println(sourceMap);
        }
    }

    static void deleteAPI(TransportClient client){
        // Delete by id
        /*
        DeleteResponse deleteResponse = client.prepareDelete("product", "_doc", "1").get();
        System.out.println(deleteResponse.getId());
        */

        // Delete by query
        BulkByScrollResponse response =
                new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                        .filter(QueryBuilders.matchQuery("name", "Dell")) // if the data name is Dell
                        .source("product")
                        .get();
        System.out.println(response.getDeleted());
    }

    public static void main(String[] args) {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch").build();

            // Connection
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

            // Add data
            indexAPI(client);

            // Get data
            /*
            getAPI(client);
            */

            // Search
            /*
            searchAPI(client);
            */

            // Delete
            /*
            deleteAPI(client);
            */
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
