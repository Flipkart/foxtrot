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

function FqlTable () {
    this.typeName = "fql_table";
    this.refresh = true;
    this.setupModalName = "#setupFqlTableModal";
    this.query = null;
    this.url = "/foxtrot/v1/fql";
    this.contentType = 'text/plain';
}

FqlTable.prototype = new Tile();

FqlTable.prototype.render = function (data, animate) {
    if (this.title){
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Query : " + this.query);
    }
    var parent = $("#content-for-" + this.id);

    if (0 != parent.find(".dataview-table").length){
        parent.find(".dataview-table").remove()
    }
    var headerData = data.headers;
    var headers = [];
    for(var i = 0; i < headerData.length; i++) {
        headers.push(headerData[i]['name']);
    }
    var rowData = data.rows;
    var rows = [];
    for(i = 0; i < rowData.length; i++) {
        var row = [];
        for(var j = 0; j < headers.length; j++) {
            row.push(rowData[i][headers[j]]);
        }
        rows.push(row);
    }
    var tableData = {headers : headers, data: rows};
    parent.append(handlebars('#table-template', tableData))
};

FqlTable.prototype.getQuery = function () {
    return this.query;
};

FqlTable.prototype.isSetupDone = function () {
    return this.query;
};

FqlTable.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.title = modal.find(".tile-title").val()
    this.query = modal.find("#fql_query").val();
};

FqlTable.prototype.populateSetupDialog = function () {
    var modal = $(this.setupModalName);
    modal.find(".tile-title").val(this.title)
    if (this.query){
        modal.find("#fql_query").val(this.query)
    }
};

FqlTable.prototype.registerSpecificData = function (representation) {
    representation['fql_query'] = this.query;
};

FqlTable.prototype.loadSpecificData = function (representation) {
    this.query = representation['fql_query'];
};

