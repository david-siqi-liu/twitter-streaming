package twitterstreaming.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import twitterstreaming.object.Tweet;

import java.util.Iterator;

public class HashtagFlatMap implements FlatMapFunction<Tweet, Tuple2<String, Integer>> {

    /**
     * Tokenize hashtags and emit each hashtag as (hashtag, 1)
     */
    @Override
    public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // Hashtags
        if (value.getHashtags() != null) {

            for (String result : value.getHashtags()) {
                if (!result.equals("")) {
                    out.collect(new Tuple2<>(result.toLowerCase(), 1));
                }
            }
        }
    }
}

