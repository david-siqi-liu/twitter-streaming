#!/bin/bash

# Word Count Index
curl -X DELETE -allow_no_indices "localhost:9200/word-count-index/"

curl -X PUT "localhost:9200/word-count-index?pretty" -H 'Content-Type: application/json' -d'
{}
'
curl -X PUT "localhost:9200/word-count-index/_mapping/_doc?pretty" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "word": {
      "type": "keyword"
    },
    "count": {
      "type": "long"
    }
  }
}
'

# Hashtag Count Index
curl -X DELETE -allow_no_indices "localhost:9200/hashtag-count-index/"

curl -X PUT "localhost:9200/hashtag-count-index?pretty" -H 'Content-Type: application/json' -d'
{}
'
curl -X PUT "localhost:9200/hashtag-count-index/_mapping/_doc?pretty" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "hashtag": {
      "type": "keyword"
    },
    "count": {
      "type": "long"
    }
  }
}
'

# Tweet Type Count Index
curl -X DELETE -allow_no_indices "localhost:9200/type-count-index/"

curl -X PUT "localhost:9200/type-count-index?pretty" -H 'Content-Type: application/json' -d'
{}
'
curl -X PUT "localhost:9200/type-count-index/_mapping/_doc?pretty" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "tweet_type": {
      "type": "keyword"
    },
    "timestamp": {
      "type": "date"
    },
    "count": {
      "type": "long"
    }
  }
}
'

# GeoMap Count Index
curl -X DELETE -allow_no_indices "localhost:9200/geomap-count-index/"

curl -X PUT "localhost:9200/geomap-count-index?pretty" -H 'Content-Type: application/json' -d'
{}
'
curl -X PUT "localhost:9200/geomap-count-index/_mapping/_doc?pretty" -H 'Content-Type: application/json' -d'
{
  "properties" : {
    "location" : {
      "type" : "geo_point"
    },
    "count" : {
      "type" : "long"
    }
  }
}
'