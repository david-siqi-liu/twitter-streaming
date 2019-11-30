package twitterstreaming.object;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Tweet {

    private String timestamp;
    private long id;
    private String text;
    private boolean retweet;
    private long user_id;
    private List<String> hashtags;
    private Tuple2<Float, Float> coordinates;
    private int favorite_count;

    public Tweet(JsonNode jsonNode) {
        this.timestamp = jsonNode.get("created_at").asText();
        this.id = jsonNode.get("id").asLong();
        this.text = jsonNode.get("text").asText();
        this.retweet = jsonNode.has("retweeted_status");
        this.user_id = jsonNode.get("user").get("id").asLong();
        if (jsonNode.has("entities") &&
                jsonNode.get("entities").has("hashtags") &&
                jsonNode.get("entities").get("hashtags").has(0)) {
            List<String> hashtags = new ArrayList<>();
            for (JsonNode i : jsonNode.get("entities").get("hashtags")) {
                hashtags.add(i.get("text").asText());
            }
            this.hashtags = hashtags
                    .stream()
                    .distinct()
                    .collect(Collectors.toList());
        }
        if (jsonNode.has("coordinates") &&
                jsonNode.get("coordinates").has("coordinates") &&
                jsonNode.get("coordinates").get("coordinates").has(0) &&
                jsonNode.get("coordinates").get("coordinates").has(1)) {
            // Twitter uses <lon, lat> while ES uses <lat, lon>
            float lon = jsonNode.get("coordinates").get("coordinates").get(0).floatValue();
            float lat = jsonNode.get("coordinates").get("coordinates").get(1).floatValue();
            this.coordinates = new Tuple2<>(lat, lon);
        } else if (jsonNode.has("place") &&
                jsonNode.get("place").has("bounding_box") &&
                jsonNode.get("place").get("bounding_box").has("coordinates") &&
                jsonNode.get("place").get("bounding_box").get("coordinates").has(0)) {
            JsonNode bounding_box = jsonNode.get("place").get("bounding_box").get("coordinates");
            float lon = 0;
            float lat = 0;
            for (JsonNode p : bounding_box.get(0)) {
                lon = lon + p.get(0).floatValue();
                lat = lat + p.get(1).floatValue();
            }
            this.coordinates = new Tuple2<>(lat / 4, lon / 4);
        }
        if (jsonNode.has("favorite_count")) {
            this.favorite_count = jsonNode.get("favorite_count").asInt();
        } else {
            this.favorite_count = 0;
        }
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public long getId() {
        return this.id;
    }

    public String getText() {
        return this.text;
    }

    public boolean getRetweet() {
        return this.retweet;
    }

    public long getUserId() {
        return this.user_id;
    }

    public List<String> getHashtags() {
        return this.hashtags;
    }

    public Tuple2<Float, Float> getCoordinates() {
        return this.coordinates;
    }

    public int getFavoriteCount() {
        return this.favorite_count;
    }

}
