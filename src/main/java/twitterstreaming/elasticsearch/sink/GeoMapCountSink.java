package twitterstreaming.elasticsearch.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import twitterstreaming.elasticsearch.Client;

public class GeoMapCountSink implements ElasticsearchSinkFunction
        <Tuple2<Tuple2<Float, Float>, Integer>> {

    private String indexName;
    private String docName;
    private Integer docId;

    public GeoMapCountSink(String indexName, String docName) {
        this.indexName = indexName;
        this.docName = docName;
        this.docId = 1;
    }

    @Override
    public void process(Tuple2<Tuple2<Float, Float>, Integer> t, RuntimeContext ctx,
                        RequestIndexer indexer) {
        try {
            XContentBuilder json = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("location")
                    .field("lat", String.format("%.2f", t.f0.f0))
                    .field("lon", String.format("%.2f", t.f0.f1))
                    .endObject()
                    .field("count", t.f1)
                    .endObject();
            IndexRequest indexRequest = Client.getInstance().requestIndex(this.indexName,
                    this.docName, this.docId.toString(), json);
            indexer.add(indexRequest);
            this.docId = this.docId + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
