/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 function Table(name, ttl) {
	this.name = name;
	this.ttl = ttl;
}

function Tables() {
	this.tables = [];
	this.tableChangeHandlers = []
	this.selectedTable = null;
	this.currentTableFieldMappings = null;
	this.metaLoadHandlers = [];
	this.initialSelectedTable = null;
}

Tables.prototype.init = function(callback) {
  var tableJson = [
  {
    "name": "csp",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "egv",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "flipcast",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "foobar",
    "ttl": 30,
    "seggregatedBackend": true
  },
  {
    "name": "fpapp",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "kratos",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "mercury",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "nexus",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "payment",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "phonepe",
    "ttl": 30,
    "seggregatedBackend": true
  },
  {
    "name": "phonepe_consumer_app_android",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "phonepe_consumer_app_android_extras",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "plutus",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "promotions",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "userservice",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "wallet",
    "ttl": 180,
    "seggregatedBackend": true
  },
  {
    "name": "yblfundtransfer",
    "ttl": 180,
    "seggregatedBackend": true
  }
];

  this.tables = [];
  for (var i = tableJson.length - 1; i >= 0; i--) {
		var table = tableJson[i];
		this.tables.push(new Table(table.name, table.ttl));
	};
	/*$.ajax({
		url: hostDetails.url("/foxtrot/v1/tables"),
		contentType: "application/json",
		context: this,
		success: function(tables) {
			this.tables = [];
			for (var i = tables.length - 1; i >= 0; i--) {
				var table = tables[i];
				this.tables.push(new Table(table.name, table.ttl));
			};
			this.selectedTable = this.tables[this.getSelectionIndex()];
			for (var i = this.tableChangeHandlers.length - 1; i >= 0; i--) {
				this.tableChangeHandlers[i](this.tables);
			};
		}
	});*/
};

Tables.prototype.loadTableMeta = function (table, callback) {
    callback = callback || $.noop;
    $.ajax({
        url: "http://foxtrot.traefik.prod.phonepe.com/foxtrot/v1/tables/" + table.name + "/fields",
        contentType: "application/json",
        context: this,
        success: $.proxy(function (data) {
            table.mappings = data.mappings ? data.mappings : [];
            this.currentTableFieldMappings = data.mappings;
            if (this.currentTableFieldMappings) {
                this.currentTableFieldMappings.sort(function (lhs, rhs) {
                    return ((lhs.field > rhs.field) ? 1 : ((lhs.field < rhs.field) ? -1 : 0));
                });
            }
            for (var i = this.metaLoadHandlers.length - 1; i >= 0; i--) {
                this.metaLoadHandlers[i](this.tables);
              console.log(this.metaLoadHandlers)
            }
            callback();
        }, this)
    });
};
