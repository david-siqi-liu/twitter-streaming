package twitterstreaming.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import twitterstreaming.object.Tweet;

public class TweetMap implements MapFunction<String, Tweet> {
    private transient ObjectMapper jsonParser;

    /**
     * Return a Tweet object
     */
    @Override
    public Tweet map(String value) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        return new Tweet(jsonNode);
    }
}
