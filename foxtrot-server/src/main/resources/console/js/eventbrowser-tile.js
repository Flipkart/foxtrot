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

 function EventBrowser () {
	this.typeName="eventbrowser";
	this.refresh = true;
	this.setupModalName = null;
	this.setupModalName = "#setupEventBrowser";
	//Instance properties
	this.count = 0;
	this.from = 0;
	this.renderingFunction = null;
}

EventBrowser.prototype = new Tile();

EventBrowser.prototype.render = function(data, animate) {
	if(!data.hasOwnProperty("documents") || data.documents.length == 0) {
		return;
	}
	var parent = $("#content-for-" + this.id);
	var headers = [];
	var headerMap = new Object();
	var rows = [];
	var flatRows = [];

	for (var i = data.documents.length - 1; i >= 0; i--) {
		var flatObject = flat.flatten(data.documents[i]);
		for(field in flatObject) {
			if(flatObject.hasOwnProperty(field)) {
				headerMap[field]=1;
			}
		}
		flatRows.push(flatObject);
	}
	headers = Object.keys(headerMap);
	for (var i = flatRows.length - 1; i >= 0; i--) {
		var row = [];
		var flatData = flatRows[i];
		for (var j = 0; j < headers.length; j++) {
			var header = headers[j];
			if(flatData.hasOwnProperty(header)) {
				row.push(flatData[header]);
			}
			else {
			    console.log("Here for " + header);
				row.push("");
			}
		}
		rows.push(row);
	}
    for (var j = 0; j < headers.length; j++) {
        headers[j] = headers[j].replace("data.","");
    }
	var tableData = {headers : headers, data: rows};
	console.log(tableData);
	parent.html(handlebars("#eventbrowser-template", tableData));
};

EventBrowser.prototype.getQuery = function() {
	if(this.count == 0) {
		return null;
	}
	return JSON.stringify({
		opcode : "query",
		table : this.tables.selectedTable.name,
		sort : {
			field:"_timestamp",
			order:"desc"
		},
		from:0,
		limit:this.count
	});
};

EventBrowser.prototype.isSetupDone = function() {
	return this.count != 0;	
};

EventBrowser.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.count = parseInt(modal.find(".event-count").val());
	this.title = modal.find(".tile-title").val()
	// this.period = parseInt(modal.find(".refresh-period").val());
	// this.eventTypeFieldName = modal.find(".bar-chart-field").val();
};

EventBrowser.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	modal.find(".event-count").val(( 0 != this.count)?this.count:"");
	modal.find(".tile-title").val(this.title)
}

EventBrowser.prototype.registerSpecificData = function(representation) {
	representation['count'] = this.count;
};

EventBrowser.prototype.loadSpecificData = function(representation) {
	this.count = representation['count'];
};