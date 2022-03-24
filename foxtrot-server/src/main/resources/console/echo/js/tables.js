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
var apiUrl = getHostUrl();

 function Table(name, ttl, customFieldMappings) {
  this.name = name;
  this.ttl = ttl;
  this.customFieldMappings = customFieldMappings
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
  isLoggedIn();
  $.ajax({
    url: apiUrl+"/v1/tables/",
    contentType: "application/json",
    context: this,
    success: function(tables) {
      this.tables = [];
      for (var i = tables.length - 1; i >= 0; i--) {
        var table = tables[i];
        this.tables.push(new Table(table.name, table.ttl, table.customFieldMappings));
      };
      this.selectedTable = this.tables[this.getSelectionIndex()];
      tableList = this.tables.tables;
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
    url: apiUrl+"/v1/tables/" + table.name + "/fields",
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
      currentFieldList = data.mappings;
      tableFiledsArray[table.name] = data;
      clearModalfields();
      for (var i = this.metaLoadHandlers.length - 1; i >= 0; i--) {
        this.metaLoadHandlers[i](this.tables);
      }
      callback();
      var selectedConsole = $("#chart-type").val();
  if(selectedConsole === "funnel") {
  $("#funnel-field option").each(function(index, value)
  {
    
    if (value.text === "eventType") {
      $('#funnel-field').val(value.value);
      $('#funnel-field').selectpicker('refresh');
    }
  });
}
    }, this)
  });
};

