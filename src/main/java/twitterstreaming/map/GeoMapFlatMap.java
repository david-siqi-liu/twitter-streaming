package twitterstreaming.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import twitterstreaming.object.Tweet;

public class GeoMapFlatMap implements FlatMapFunction<Tweet, Tuple2<Tuple2<Float, Float>, Integer>> {

    /**
     * Tokenize hashtags and emit each hashtag as (hashtag, 1)
     */
    @Override
    public void flatMap(Tweet value, Collector<Tuple2<Tuple2<Float, Float>, Integer>> out) throws Exception {
        // Hashtags
        Tuple2<Float, Float> location = value.getCoordinates();

        if (location != null) {
            out.collect(new Tuple2<>(location, 1));
        }
    }
}
