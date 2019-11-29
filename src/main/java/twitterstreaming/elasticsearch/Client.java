package twitterstreaming.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;


/**
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.4/java-rest-high-supported-apis.html
 */

public class Client {

    private static Client instance = null;
    private RestHighLevelClient client;

    private Client() {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));
    }

    public static Client getInstance() {
        if (instance == null) {
            instance = new Client();
        }
        return instance;
    }

    /*
    IndexRequest request = new IndexRequest(
        "index",
        "type",
        "docId");
    request.source(docSource, XContentType.JSON)
     */
    public IndexRequest requestIndex(String indexName, String typeName, String docId, XContentBuilder json) {

        return Requests.indexRequest()
                .index(indexName)
                .type(typeName)
                .id(docId)
                .source(json);
    }
}
