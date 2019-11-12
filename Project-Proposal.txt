Zibai Wang, zibai.wang@uwaterloo.ca, 20819005
Siqi Liu, sq2liu@uwaterloo.ca, 20428295

# Project Proposal

Our group will be on topic #2 - learn and explore a (new) big data processing framework.

Specifically, we will be using **Apache Flink** to implement a Java-based program to **stream-process and analyze real-time Twitter data**.

First, the program takes in a query term (e.g. hashtag) from the user, and performs **real-time ETL of Tweets** that matches the given query term.

Next, various data mining tasks and **real-time analytics** are performed and presented using **interactive visualization dashboards** (either in Apache Flink Dashboard, ElasticSearch or Kibana):

- Geographic heat map (USA/Canada)
- Top 10 most frequently occured words in Tweets
- Top 10 liked Tweets
- Top 10 retweeted Tweets
- (Optional) real-time sentiment analysis (positive/negative/neutral)



*Optional Tasks (if time allowed)*

- Batch processing (given a time window, say 30 minutes, include historical Tweets as well)
- Graph analysis

# Goal

By the end of the project, we hope to

- Understand stream-processing, and how it is different from Spark's batch-processing
- Become familiar with Apache Flink, and its Twitter API
- Learn how to set-up real-time analytics dashboard