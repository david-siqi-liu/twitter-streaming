# CS 651 Final Project
## Links

https://rawkintrevo.org/2016/09/30/big-data-for-n00bs-my-first-streaming-flink-program-twitter/

https://www.novatec-gmbh.de/en/blog/sentimentanalyzer/

https://blog.brakmic.com/stream-processing-with-apache-flink/

https://towardsdatascience.com/real-time-twitter-sentiment-analysis-for-brand-improvement-and-topic-tracking-chapter-1-3-e02f7652d8ff

https://github.com/godatadriven/flink-streaming-xke

## How To Run

1. Start local cluster

```bash
cd /usr/local/Cellar/apache-flink/1.9.1

./libexec/bin/start-local.sh
```

2. Web UI http://localhost:8081/
3. Build project

```bash
cd ~/Github/twitter-streaming/

mvn clean package
```

3. Start job

```bash
cd /usr/local/Cellar/apache-flink/1.9.1

nc -l 9000

flink run -c twitterstreaming.TwitterStream ~/Github/twitter-streaming/target/twitter-streaming-1.0.jar --port 9000
```

