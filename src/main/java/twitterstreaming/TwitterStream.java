package twitterstreaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import twitterstreaming.object.*;
import twitterstreaming.map.*;
import twitterstreaming.util.TwitterExampleData;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 */
public class TwitterStream {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample " +
                "[--output <path>]" +
                "[--twitter-source.consumerKey <key>]" +
                "[--twitter-source.consumerSecret <secret>]" +
                "[--twitter-source.token <token>]" +
                "[--twitter-source.tokenSecret <tokenSecret>]");

        // Time window
        Time windowSize = Time.seconds(5);

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // Get input data
        DataStream<String> streamSource;
        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)
        ) {
            streamSource = env.addSource(new TwitterSource(params.getProperties()));
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
            // get default test text data
            streamSource = env.fromElements(TwitterExampleData.TEXTS);
        }

        // Get tweets, store in Tweet objects
        DataStream<Tweet> tweets = streamSource
                .map(new TweetMap());

        // Create Tuple2 <text, integer>, word counts
        DataStream<Tuple2<String, Integer>> wordCountSum = tweets
                .flatMap(new TextTokenizeFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Emit result
        if (params.has("output")) {
            wordCountSum.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            wordCountSum.print();
        }

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // Use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                    public IndexRequest createIndexRequest(Tuple2<String, Integer> t) throws IOException {
                        XContentBuilder builder = XContentFactory.jsonBuilder()
                                .startObject()
                                .field("word", t.f0)
                                .field("count", t.f1)
                                .endObject();

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(builder, XContentType.JSON);
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
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );

        // Finally, build and add the sink to the job's pipeline
        wordCountSum.addSink(esSinkBuilder.build());

        // Execute program
        env.execute("Twitter Stream");
    }
}
