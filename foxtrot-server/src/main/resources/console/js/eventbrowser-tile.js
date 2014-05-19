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
	if(data.documents.length == 0) {
		return;
	}
	var parent = $("#content-for-" + this.id);
	var headers = [];
	var headerMap = new Object();
	// for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
	// 	headers.push(this.tables.currentTableFieldMappings[i].name);
	// 	headerMap[this.tables.currentTableFieldMappings[i].name] = null;
	// }
	var rows = [];
	var flatRows = [];

	for (var i = data.documents.length - 1; i >= 0; i--) {
		var flatObject = flat.flatten(data.documents[i]);
		//console.log(flatObject);
		for(field in flatObject) {
			if(flatObject.hasOwnProperty(field)) {
				headerMap[field]=1;
			}
		}
		flatRows.push(flatObject);
	}
	headers = Object.keys(headerMap);
	headers.sort()
	for (var i = flatRows.length - 1; i >= 0; i--) {
		var row = [];
		var flatData = flatRows[i];
		for (var j = 0; j < headers.length - 1; j++) {
			var header = headers[j];
			if(flatData.hasOwnProperty(header)) {
				row.push(flatData[header]);
			}
			else {
				row.push("");
			}
		}
		rows.push(row);
	}
	var tableData = {headers : headers, data: rows};
	console.log(tableData);
	parent.html(handlebars("#eventbrowser-template", tableData));	
	// var chartLabel = null;
	// if(0 == parent.find(".pielabel").length) {
	// 	chartLabel = $("<div>", {class: "pielabel"});
	// 	parent.append(chartLabel);
	// }
	// else {
	// 	chartLabel = parent.find(".pielabel");
	// }
	// chartLabel.text((this.period >= 60) ? ((this.period / 60) + "h"): (this.period + "m"));

	// var canvas = null;
	// if(0 == parent.find(".chartcanvas").length) {
	// 	canvas = $("<div>", {class: "chartcanvas"});
	// 	parent.append(canvas);
	// }
	// else {
	// 	canvas = parent.find(".chartcanvas");
	// }
	// var colors = new Colors(Object.keys(data.result).length);
	// var columns =[];
	// var ticks = [];
	// var i = 0;
	// for(property in data.result) {
	// 	columns.push({label: property, data: [[i, data.result[property]]], color: colors.nextColor()});
	// 	ticks.push([i, property]);
	// 	i++;
	// }

	// var chartOptions = {
 //        series: {
 //            bars: {
 //                show: true,
 //                label:{
 //                    show: true
 //                },
 //                barWidth: 0.5,
 //            	align: "center",
 //            	lineWidth: 0,
 //            	fill: 1.0
 //            }
 //        },
 //        legend : {
 //            show: false
 //        },
 //        xaxis : {
 //        	ticks: ticks,
 //        	tickLength: 0        	
 //        },
 //        yaxis: {
 //        	tickLength: 0
 //        },
 //        grid: {
 //        	hoverable: true,
 //        	borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
 //        },
 //        tooltip: true,
 //        tooltipOpts: {
 //    		content: function(label, x, y) {
 //    			return label + ": " + y;
 //    		}
 //    	}
 //    };
 //    $.plot(canvas, columns, chartOptions);
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
			order:"asc"
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
	// this.period = parseInt(modal.find(".refresh-period").val());
	// this.eventTypeFieldName = modal.find(".bar-chart-field").val();
};

EventBrowser.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	modal.find(".event-count").val(( 0 != this.count)?this.count:"");	
}

EventBrowser.prototype.registerSpecificData = function(representation) {
	representation['count'] = this.count;
};

EventBrowser.prototype.loadSpecificData = function(representation) {
	this.count = representation['count'];
};