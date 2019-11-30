package twitterstreaming;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import twitterstreaming.elasticsearch.sink.GeoMapCountSink;
import twitterstreaming.elasticsearch.sink.HashtagCountSink;
import twitterstreaming.elasticsearch.sink.TweetTypeCountSink;
import twitterstreaming.elasticsearch.sink.WordCountSink;
import twitterstreaming.map.*;
import twitterstreaming.object.Tweet;
import twitterstreaming.util.TwitterFilterEndpoint;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the "TwitterStream" program
 */
public class TwitterStream {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Time window
        Time windowSize = Time.seconds(5);

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // *************************************************************************
        // DATA STREAM
        // *************************************************************************

        // Get input data
        TwitterSource twittersource = new TwitterSource(params.getProperties());

        // Add tracked term(s), if specified
        // Filter languages (e.g. "en") and North America, if specified
        if (params.has("track") || params.has("language") || params.has("onlyna")) {
            TwitterFilterEndpoint filterEndpoint = new TwitterFilterEndpoint();

            if (params.has("track")) {
                filterEndpoint.AddTrackTerms(params.get("track").split(","));
            }
            if (params.has("language")) {
                filterEndpoint.AddLanguages(params.get("language").split(","));
            }
            if (params.has("onlyna")) {
                filterEndpoint.AddNAOnly();
            }

            twittersource.setCustomEndpointInitializer(filterEndpoint);
        }

        // Stream source
        DataStream<String> streamSource = env.addSource(twittersource);

        // Get tweets, store in Tweet objects
        DataStream<Tweet> tweets = streamSource
                .map(new TweetMap());

        // Create Tuple2 <String, Integer> of <Word, Count>
        DataStream<Tuple2<String, Integer>> wordCount = tweets
                .flatMap(new WordFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Create Tuple2 <String, Integer> of <Word, Count>
        DataStream<Tuple2<String, Integer>> hashtagCount = tweets
                .flatMap(new HashtagFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Create Tuple3<String, Long, Integer> of <Tweet/Retweet, Time, Count>
        DataStream<Tuple3<String, Long, Integer>> tweetTypeCount = tweets
                .flatMap(new TweetTypeCountFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable,
                                        Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        int count = 0;
                        String type = "";
                        for (Tuple2<String, Integer> item : iterable) {
                            type = item.f0;
                            count += item.f1;
                        }
                        collector.collect(new Tuple3<>(type, context.window().getStart(), count));
                    }
                });

        // Create Tuple2 <Tuple2<Float, Float>, Integer> of <Location, Count>
        DataStream<Tuple2<Tuple2<Float, Float>, Integer>> geomapCount = tweets
                .flatMap(new GeoMapFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Emit result
        if (params.has("output")) {
            wordCount.writeAsText(params.get("output") + "wordCount.txt", FileSystem.WriteMode.OVERWRITE);
            hashtagCount.writeAsText(params.get("output") + "hashtagCount.txt", FileSystem.WriteMode.OVERWRITE);
            tweetTypeCount.writeAsText(params.get("output") + "tweetTypeCount.txt", FileSystem.WriteMode.OVERWRITE);
            geomapCount.writeAsText(params.get("output") + "geomapCount.txt", FileSystem.WriteMode.OVERWRITE);
        }

        // *************************************************************************
        // ELASTICSEARCH
        // *************************************************************************

        // Hosts
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // Create an ElasticsearchSink for wordCount
        ElasticsearchSink.Builder<Tuple2<String, Integer>> wordCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new WordCountSink("word-count-index", "_doc")
        );

        // Create an ElasticsearchSink for hashtagCount
        ElasticsearchSink.Builder<Tuple2<String, Integer>> hashtagCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new HashtagCountSink("hashtag-count-index", "_doc")
        );

        // Create an ElasticsearchSink for tweetTypeCount
        ElasticsearchSink.Builder<Tuple3<String, Long, Integer>> tweetTypeCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new TweetTypeCountSink("type-count-index", "_doc")
        );

        // Create an ElasticsearchSink for favouriteCount
        ElasticsearchSink.Builder<Tuple2<Tuple2<Float, Float>, Integer>> geomapCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new GeoMapCountSink("geomap-count-index", "_doc")
        );

        // Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        wordCountSink.setBulkFlushMaxActions(1);
        hashtagCountSink.setBulkFlushMaxActions(1);
        tweetTypeCountSink.setBulkFlushMaxActions(1);
        geomapCountSink.setBulkFlushMaxActions(1);

        // Finally, build and add the sink to the job's pipeline
        wordCount.addSink(wordCountSink.build());
        hashtagCount.addSink(hashtagCountSink.build());
        tweetTypeCount.addSink(tweetTypeCountSink.build());
        geomapCount.addSink(geomapCountSink.build());

        // Execute program
        env.execute("Twitter Stream");
    }
}
