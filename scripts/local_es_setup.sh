#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters, Usage :- ./local_es_setup.sh <es_host_ip> <es_table_name_prefix_for_index>"
   exit 1
fi

curl -H 'Content-type: application/json' -XPUT ${1}:9200/_template/template_foxtrot_mappings -d '
{
  "template": "'${2}'-*",
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 0
  },
  "mappings": {
    "document": {
      "_source": {
        "enabled": false
      },
      "_all": {
        "enabled": false
      },
      "dynamic_templates": [
        {
          "template_metadata_timestamp": {
            "match": "__FOXTROT_METADATA__.time",
            "mapping": {
              "store": true,
              "index": "true",
              "type": "date"
            }
          }
        },
       {
          "template_metadata_string": {
            "match": "__FOXTROT_METADATA__.*",
            "match_mapping_type": "string",
            "mapping": {
              "store": true,
              "index": "true",
              "type": "text"
            }
          }
        },
        {
          "template_metadata_others": {
            "match": "__FOXTROT_METADATA__.*",
            "mapping": {
              "store": true,
              "index": "true"
            }
          }
        },
        {
          "template_object_store_analyzed": {
            "match": "*",
            "match_mapping_type": "object",
            "mapping": {
              "index": "true"
            }
          }
        },
        {
          "template_no_store_analyzed": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "index": "true",
              "type": "keyword",
              "fields": {
                "analyzed": {
                  "type": "text",
                  "index": "true"
                }
              }
            }
          }
        },
        {
          "template_no_store": {
            "match_mapping_type": "*",
            "match_pattern": "regex",
            "path_match": ".*",
            "mapping": {
              "index": "true"
            }
          }
        }
      ],

                "properties": {
                    "__FOXTROT_METADATA__": {
                        "properties": {
                            "time": {
                                "type": "date",
                                "index": "true"
                            }
                        }
                    }
                }
    }
  }
}'

curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/consoles/" -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 0
        }
    }
}'

curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/table-meta/" -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 0
        }
    }
}'
