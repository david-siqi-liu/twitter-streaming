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
    private boolean retweeted;
    private long user_id;
    private List<String> hashtags;
    private Tuple2<Float, Float> coordinates;
    private int favorite_count;

    public Tweet(JsonNode jsonNode) {

        this.timestamp = jsonNode.get("created_at").asText();
        this.id = jsonNode.get("id").asLong();
        this.text = jsonNode.get("text").asText();
        this.retweeted = jsonNode.has("retweeted_status");
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
//            this.coordinates = new float[2];
//            this.coordinates[0] = jsonNode.get("coordinates").get("coordinates").get(0).floatValue();
//            this.coordinates[1] = jsonNode.get("coordinates").get("coordinates").get(1).floatValue();
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

    public boolean getRetweeted() {
        return this.retweeted;
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
