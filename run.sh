#!/bin/bash

flink run -c twitterstreaming.TwitterStream ~/Github/twitter-streaming/target/twitter-streaming-1.0.jar \
--port 9000 \
--twitter-source.consumerKey NiUWI0qY98cDBElRewdQdAXb6 \
--twitter-source.consumerSecret rMis11GJ3PVBTxfPcvOB0MZAFFZfkAR5L24132MsC7yXAfYred \
--twitter-source.token 1191147457322868736-kC8TGZ92q3UyPStgs4yRtuOW7asKfv \
--twitter-source.tokenSecret DHzxjmmzT3wmiys5t6mgNttC2n0pSp2UmckZVSOlWM8xH \
--output  ~/Github/twitter-streaming/output/test.txt