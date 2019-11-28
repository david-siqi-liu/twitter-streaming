package twitterstreaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import twitterstreaming.elasticsearch.*;
import twitterstreaming.object.*;
import twitterstreaming.map.*;
import twitterstreaming.util.*;

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 */
public class TwitterStream {

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
        Time windowSize = Time.seconds(60);

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // *************************************************************************
        // DATA STREAM
        // *************************************************************************

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

        // Create Tuple2 <String, Integer> of <Word, Count>
        DataStream<Tuple2<String, Integer>> wordCount = tweets
                .flatMap(new TextTokenizeFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Create Tuple2 <String, Integer> of <Hashtag, Count>
        DataStream<Tuple2<String, Integer>> hashtagCount = tweets
                .flatMap(new HashtagFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // Emit result
        if (params.has("output")) {
            wordCount.writeAsText(params.get("output") + "wordCount.txt", FileSystem.WriteMode.OVERWRITE);
            hashtagCount.writeAsText(params.get("output") + "hashtagCount.txt", FileSystem.WriteMode.OVERWRITE);
        }

        // *************************************************************************
        // ELASTICSEARCH
        // *************************************************************************

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // Create an ElasticsearchSink for wordCount
        ElasticsearchSink.Builder<Tuple2<String, Integer>> wordCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new WordCountSink()
        );

        // Create an ElasticsearchSink for hashtagCount
        ElasticsearchSink.Builder<Tuple2<String, Integer>> hashtagCountSink = new ElasticsearchSink.Builder<>(
                httpHosts,
                new HashtagCountSink()
        );

        // Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        wordCountSink.setBulkFlushMaxActions(1);
        hashtagCountSink.setBulkFlushMaxActions(1);

        // Finally, build and add the sink to the job's pipeline
        wordCount.addSink(wordCountSink.build());
        hashtagCount.addSink(hashtagCountSink.build());

        // Execute program
        env.execute("Twitter Stream");
    }
}
