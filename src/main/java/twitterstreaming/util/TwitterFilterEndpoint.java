package twitterstreaming.util;

// reference https://github.com/vriesdemichael/twitter-flink/blob/dc67adc4d8ed7f0dcdf3747c49ba7d6a889e1116/src/main/java/FilterEndpoint.java

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;

import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;



public class TwitterFilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

    private ArrayList<String> trackTerms;

    public TwitterFilterEndpoint() {
        this.trackTerms = new ArrayList<>();
    }

    public void addTrackTerm(String... trackTerm) {
        Collections.addAll(this.trackTerms, trackTerm);
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint(false);

        /*

        endpoint.locations(Arrays.asList(
  
                                new Location(
                                        // north america: -168.48633, 13.23995 -50.36133, 72.76406
                                        new Location.Coordinate(-168.48633, 13.23995), // south west
                                        new Location.Coordinate(-50.36133, 72.76406))
                        )
                );
        */
        endpoint.languages(Arrays.asList("en"));
        
        if (trackTerms.size() > 0) {
            endpoint.trackTerms(trackTerms);
        }

    
        endpoint.stallWarnings(false);
        endpoint.delimited(false);
    
        return endpoint;
    }

}