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
	$.ajax({
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
	});
};

Tables.prototype.forceSelectedTableAfterInit = function(tableName) {
    this.initialSelectedTable = tableName;
}

Tables.prototype.getSelectionIndex = function() {
    if(!this.initialSelectedTable) {
        return 0;
    }
    for (var i = 0; i < this.tables.length; i++) {
            if(this.tables[i].name === this.initialSelectedTable) {
            return i;
        }
    }
}

Tables.prototype.registerTableChangeHandler = function(tableChangeHandler) {
	this.tableChangeHandlers.push(tableChangeHandler);
};

Tables.prototype.registerMetaLoadHandler = function(metaLoadHandler) {
	this.metaLoadHandlers.push(metaLoadHandler);
};

Tables.prototype.loadTableMeta = function (table, callback) {
    callback = callback || $.noop;
    $.ajax({
        url: hostDetails.url("/foxtrot/v1/tables/" + table.name + "/fields"),
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
            }
            callback();
        }, this)
    });
};
