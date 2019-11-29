package twitterstreaming.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import twitterstreaming.object.Tweet;

public class FavouriteCountFlatMap implements FlatMapFunction<Tweet, Tuple2<String, Integer>> {

    /**
     * emit (tweet text, favourite count)
     */
    @Override
    public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // Hashtags
        Integer favouriteCount = value.getFavoriteCount();
        String text = value.getText();

        if (favouriteCount != 0) {
            out.collect(new Tuple2<>(text, favouriteCount));
        }
    }
}
