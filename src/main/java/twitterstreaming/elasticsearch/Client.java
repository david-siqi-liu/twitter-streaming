package twitterstreaming.elasticsearch;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


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
