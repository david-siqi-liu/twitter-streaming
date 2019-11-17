package twitterstreaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.StringTokenizer;

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

		// Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // Get tweets
		DataStream<Tuple2<String, Integer>> tweets = streamSource
				// Selecting English tweets and splitting to (word, 1)
				.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// Group by words and sum their occurrences
				.keyBy(0)
				.sum(1);

		// Emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// Execute program
		env.execute("Twitter Stream");
    }

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Deserialize JSON from twitter source
	 *
	 * <p>Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
			boolean hasText = jsonNode.has("text");
			if (isEnglish && hasText) {
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
			}
		}
	}
}
