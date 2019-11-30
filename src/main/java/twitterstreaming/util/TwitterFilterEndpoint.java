package twitterstreaming.util;

// reference https://github.com/vriesdemichael/twitter-flink/blob/dc67adc4d8ed7f0dcdf3747c49ba7d6a889e1116/src/main/java/FilterEndpoint.java

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;


public class TwitterFilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

    private ArrayList<String> trackTerms;
    private ArrayList<String> languages;
    private boolean onlyNA;

    public TwitterFilterEndpoint() {
        this.trackTerms = new ArrayList<>();
        this.languages = new ArrayList<>();
        this.onlyNA = false;
    }

    public void AddTrackTerms(String[] trackTerms) {
        Collections.addAll(this.trackTerms, trackTerms);
    }

    public void AddLanguages(String[] languages) {
        Collections.addAll(this.languages, languages);
    }

    public void AddNAOnly() {
        this.onlyNA = true;
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint(false);

        if (this.onlyNA) {
            endpoint.locations(Arrays.asList(
                    new Location(
                            // North America
                            new Location.Coordinate(-168.48, 13.23),
                            new Location.Coordinate(-50.36, 72.76))
                    )
            );
        }

        if (this.languages.size() > 0) {
            endpoint.languages(this.languages);
        }

        if (this.trackTerms.size() > 0) {
            endpoint.trackTerms(this.trackTerms);
        }

        endpoint.stallWarnings(false);
        endpoint.delimited(false);

        return endpoint;
    }

}