#!/bin/bash

curl -XPUT ${1}:9200/_template/template_foxtrot_table_main -d '
{
    "template" : "foxtrot-*",
    "settings" : {
        "number_of_shards" : 10,
        "number_of_replicas" : 0
    },
    "mappings" : {
        "document" : {
            "_source" : { "enabled" : false },
            "_all" : { "enabled" : false },
            "_timestamp" : { "enabled" : true },

            "dynamic_templates" : [
	            {
	                "template_timestamp" : {
	                    "match" : "timestamp",
	                    "mapping" : {
	                        "store" : false,
	                        "index" : "not_analyzed",
	                        "type" : "date"
	                    }
	                }
                },
                {
	                "template_no_store" : {
	                    "match" : "*",
	                    "mapping" : {
	                        "store" : false,
	                        "index" : "not_analyzed"
	                    }
	                }
	            }
            ]
        }
    }
}'

curl -XPUT "http://${1}:9200/consoles/" -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 2
        }
    }
}'

curl -XPUT "http://${1}:9200/table-meta/" -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 2
        }
    }
}'
