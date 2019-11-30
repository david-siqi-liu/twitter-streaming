package twitterstreaming.elasticsearch.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import twitterstreaming.elasticsearch.Client;

public class TweetTypeCountSink implements ElasticsearchSinkFunction<Tuple3<String, Long, Integer>> {

    private String indexName;
    private String docName;
    private Integer docId;

    public TweetTypeCountSink(String indexName, String docName) {
        this.indexName = indexName;
        this.docName = docName;
        this.docId = 1;
    }

    @Override
    public void process(Tuple3<String, Long, Integer> t, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            XContentBuilder json = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("tweet_type", t.f0)
                    .field("timestamp", t.f1)
                    .field("count", t.f2)
                    .endObject();
            IndexRequest indexRequest = Client.getInstance().requestIndex(this.indexName, this.docName, this.docId.toString(), json);
            indexer.add(indexRequest);
            this.docId = this.docId + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
