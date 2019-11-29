package twitterstreaming.elasticsearch.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import twitterstreaming.elasticsearch.Client;

public class FavouriteCountSink implements ElasticsearchSinkFunction<Tuple2<String, Integer>> {

    private String indexName;
    private String docName;
    private Integer docId;

    public FavouriteCountSink(String indexName, String docName) {
        this.indexName = indexName;
        this.docName = docName;
        this.docId = 1;
    }

    @Override
    public void process(Tuple2<String, Integer> t, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            XContentBuilder json = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", t.f0)
                    .field("count", t.f1)
                    .endObject();
            IndexRequest indexRequest = Client.getInstance().requestIndex(this.indexName, this.docName, this.docId.toString(), json);
            indexer.add(indexRequest);
            this.docId = this.docId + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
