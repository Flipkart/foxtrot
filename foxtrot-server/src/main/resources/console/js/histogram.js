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

 function Histogram () {
	this.typeName = "histogram";
	this.refresh = true;
	this.setupModalName = "#setupHistogram";
	//Instance properties
	this.period = 0;		
}

Histogram.prototype = new Tile();

Histogram.prototype.render = function(data, animate) {
	if(this.period == 0) {
		return;
	}
	var chartAreaId = "#content-for-" + this.id;
	var chartContent = $("#" + this.id).find(chartAreaId);
	var times = [];
	if(!data.hasOwnProperty(data)) {
		chartContent.empty();
	}
	var rows = [];
	rows.push(['date', 'count']);
	for (var i = data.counts.length - 1; i >= 0; i--) {
		rows.push([data.counts[i].period, data.counts[i].count]);
	};
	var timestamp = new Date().getTime();
	var d = { data: rows, color: "#249483" };
    $.plot(chartAreaId, [ d ], {
        series: {
            lines: {show: true, fill: true, fillColor: "#249483" }
        },
        grid: {
            hoverable: true,
            color: "white",
            show: true
        },
        xaxis: {
            mode: "time",
            timezone: "browser",
            min: timestamp - (this.period * 60000),
            max: timestamp
        },
        selection : {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        }
    });
	console.log(data);
};

Histogram.prototype.isSetupDone = function() {
	return this.period != 0;	
};

Histogram.prototype.getQuery = function() {
	if(this.period != 0) {
		var timestamp = new Date().getTime();
		// {"opcode":"histogram","table":"test-app","filters":[],"from":0,"to":1398837122311,"field":"_timestamp","period":"hours"}
		return JSON.stringify({
			opcode : "histogram",
			table : this.tables.selectedTable.name,
			from: (timestamp - (this.period * 60000)),
			to: timestamp,
			field: "_timestamp",
			period: "minutes"
		});
	}
};

Histogram.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	console.log("Config changed for: " + this.id);
};

Histogram.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);	
	modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");			
}

Histogram.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
};

Histogram.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
};