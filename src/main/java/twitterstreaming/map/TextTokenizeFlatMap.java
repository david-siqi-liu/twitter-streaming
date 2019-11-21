package twitterstreaming.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.StringTokenizer;

import twitterstreaming.object.Tweet;

public class TextTokenizeFlatMap implements FlatMapFunction<Tweet, Tuple2<String, Integer>> {

    /**
     * Tokenize Tweeter Text and emit each word as (word, 1)
     */
    @Override
    public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // Tweet text
        StringTokenizer tokenizer = new StringTokenizer(value.getText());

        while (tokenizer.hasMoreTokens()) {
            String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

            if (!result.equals("")) {
                out.collect(new Tuple2<>(result, 1));
            }
        }
    }
}
