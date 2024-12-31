 index_settings = {
        "mappings": {
            "properties": {
                "tweet_id": {"type": "keyword"},
                "text": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "hashtags": {"type": "keyword"},
                "created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                },
                "geo_coordinates": {"type": "geo_point", "null_value": "0,0"},
                "user": {
                    "properties": {
                        "user_id": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256}
                            },
                        },
                        "screen_name": {"type": "keyword"},
                        "location": {"type": "text"},
                    }
                },
                "sentiment_label": {"type": "keyword"},
                "sentiment_score": {"type", "double"},
            }
        },
    }