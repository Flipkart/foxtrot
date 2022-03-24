/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.common;


import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.util.JsonUtils;

public enum FieldDataType {
    INTEGER {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.INTEGER);
        }
    },
    LONG {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.LONG);
        }
    },
    FLOAT {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.FLOAT);
        }
    },
    DOUBLE {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.DOUBLE);
        }
    },
    BOOLEAN {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.BOOLEAN);
        }
    },
    DATE {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.DATE);
        }
    },
    NESTED {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.NESTED);
        }
    },
    KEYWORD {
        @Override
        public ObjectNode mapping() {
            ObjectNode objectNode = getObjectNode(Constants.KEYWORD);
            objectNode.put(Constants.IGNORE_ABOVE, 8000);
            return objectNode;
        }
    },
    GEOPOINT_MARKER {
        // This is a Marker DataType and doesnot affect mapping at all.
        // To Actually create a geopoint check GEOPOINT
        @Override
        public ObjectNode mapping() {
            return null;
        }
    },
    GEOPOINT {
        @Override
        public ObjectNode mapping() {
            return getObjectNode(Constants.GEOPOINT);
        }
    },
    TEXT {
        /*
        {
            "ignore_above": 8000,
            "store": false,
            "type": "keyword",
            "fields": {
                "analyzed": {
                    "type": "text"
                }
            }
        }
         */
        @Override
        public ObjectNode mapping() {
            ObjectNode objectNode = JsonUtils.createObjectNode();
            objectNode.put(Constants.IGNORE_ABOVE, 8000);
            objectNode.put(Constants.STORE, false);
            objectNode.put(Constants.TYPE, Constants.KEYWORD);

            ObjectNode childNode2 = JsonUtils.createObjectNode();
            childNode2.put(Constants.TYPE, Constants.TEXT);

            ObjectNode childNode1 = JsonUtils.createObjectNode();
            childNode1.set(Constants.ANALYZED, childNode2);

            objectNode.set(Constants.FIELDS, childNode1);
            return objectNode;
        }
    };

    private static ObjectNode getObjectNode(String type) {
        ObjectNode objectNode = JsonUtils.createObjectNode();
        objectNode.put(Constants.TYPE, type);
        return objectNode;
    }

    public abstract ObjectNode mapping();


}
