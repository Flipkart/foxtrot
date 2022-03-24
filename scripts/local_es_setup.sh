#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters, Usage :- ./local_es_setup.sh <es_host_ip> <es_table_name_prefix_for_index>"
   exit 1
fi

curl -H 'Content-type: application/json' -XPUT ${1}:9200/_template/template_foxtrot_mappings -d '
{
  "template": "'${2}'-*",
  "settings": {
            "index": {
                "number_of_shards": "2",
                "number_of_replicas": "0",
                "mapping": {
                    "total_fields": {
                        "limit": "5000"
                    }
                }
            }
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

curl -H 'Content-type: application/json' -XPUT localhost:9200/_template/console_v2 -d '
{
  "template": "consoles_v2*",
  "settings": {
            "index": {
                "number_of_shards": "1",
                "number_of_replicas": "1"
            }
        },
  "mappings": {
    "console_data": {
      "dynamic_templates": [

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
        "name": {
          "type": "text",
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        }
      }
    }
  }
}'


curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/consoles_v2/" -d '{
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
curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/tenant-meta/" -d '
{"aliases":{},"mappings":{"tenant-meta":{"properties":{"emailIds":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"tenantName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}},"settings":{"index":{"number_of_shards":"5","number_of_replicas":"1"}}}
'
curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/pipeline-meta/" -d '
{"aliases":{},"mappings":{"pipeline-meta":{"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"ignoreErrors":{"type":"boolean"}}}},"settings":{"index":{"number_of_shards":"5","number_of_replicas":"1"}}}
'



curl -H 'Content-type: application/json' -XPUT "http://${1}:9200/consoles_history/" -d '

{"aliases":{},"mappings":{"console_data":{"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"sections":{"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"templateFilter":{"properties":{"filters":{"properties":{"cachedResultsAccepted":{"type":"boolean"},"field":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"operator":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"value":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"values":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"table":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"templateFilters":{"properties":{"cachedResultsAccepted":{"type":"boolean"},"field":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"operator":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"value":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"tileData":{"type":"object","enabled":false},"tileList":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"updatedAt":{"type":"long"},"version":{"type":"long"}}}},"settings":{"index":{"number_of_shards":"5","number_of_replicas":"1"}}}
'