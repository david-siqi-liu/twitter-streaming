# CS 651 Final Project
## Links

https://rawkintrevo.org/2016/09/30/big-data-for-n00bs-my-first-streaming-flink-program-twitter/

https://www.novatec-gmbh.de/en/blog/sentimentanalyzer/

https://blog.brakmic.com/stream-processing-with-apache-flink/

https://towardsdatascience.com/real-time-twitter-sentiment-analysis-for-brand-improvement-and-topic-tracking-chapter-1-3-e02f7652d8ff

https://github.com/godatadriven/flink-streaming-xke

# Twitter endpoints:

StatusesSampleEndpoint.java: return random sample from all tweets
StatusesFilterEndpoint.java: return tweets based on filter condition, including language, followings, location, term
https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter

## How To Run

1. Start local cluster (in terminal one)

```bash
cd /usr/local/Cellar/apache-flink/1.9.1

./libexec/bin/start-cluster.sh
```

2. Web UI http://localhost:8081/
3. Build project (**better open another terminal**)

```bash
cd ~/Github/twitter-streaming/

mvn clean package
```

4. Start job (**better open another terminal**)

```bash
nc -l 9000
```

5. Run job (in first terminal)

```bash
./bin/flink run -c twitterstreaming.TwitterStream ~/Github/twitter-streaming/target/twitter-streaming-1.0.jar --port 9000
```

6. Check std output (in first terminal)

```bash
tail -f libexec/log/flink-*-taskexecutor-*.out
```

7. Stop cluster (in first terminal)

```bash
./libexec/bin/stop-cluster.sh
```

Reference list:

https://github.com/vriesdemichael/twitter-flink/blob/master/src/main/java/TwitterFilterEndpoint.java

https://www.programcreek.com/java-api-examples/?code=IIDP/OSTMap/OSTMap-master/stream_processing/src/main/java/org/iidp/ostmap/stream_processing/GeoTwitterSource.java
