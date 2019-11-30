package twitterstreaming.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import twitterstreaming.object.Tweet;

public class TweetTypeCountFlatMap implements FlatMapFunction<Tweet, Tuple2<String, Integer>> {
    /**
     * Retweet/Tweet
     */
    @Override
    public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> out) throws Exception {
        if (value.getRetweet()) {
            out.collect(new Tuple2<>("Retweet", 1));
        } else {
            out.collect(new Tuple2<>("Tweet", 1));
        }
    }
}
