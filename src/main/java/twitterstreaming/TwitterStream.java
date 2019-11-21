package twitterstreaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import twitterstreaming.object.*;
import twitterstreaming.map.*;
import twitterstreaming.util.TwitterExampleData;

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

        // Execute program
        env.execute("Twitter Stream");
    }
}
