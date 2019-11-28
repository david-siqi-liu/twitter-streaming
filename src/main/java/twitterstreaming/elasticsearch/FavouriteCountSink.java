package twitterstreaming.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class FavouriteCountSink implements ElasticsearchSinkFunction<Tuple2<String, Integer>> {

    private IndexRequest createIndexRequest(Tuple2<String, Integer> t) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("tweettext", t.f0)
                .field("count", t.f1)
                .endObject();

        return Requests.indexRequest()
                .index("favourite-count-index")
                .type("favourite-count-by-timestamp")
                .source(builder);
    }

    @Override
    public void process(Tuple2<String, Integer> t, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            indexer.add(createIndexRequest(t));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
