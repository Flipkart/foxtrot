package com.flipkart.foxtrot.sql.responseprocessors.model;

public class MetaData {
        private Object data;
        private int length;

        public MetaData(Object data, int length) {
            this.data = data;
            this.length = length;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }
    }
