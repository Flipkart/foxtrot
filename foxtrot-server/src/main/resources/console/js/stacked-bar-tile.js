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

function StackedBar () {
	this.typeName = "stacked_bar";
	this.refresh = true;
	this.setupModalName = "#setupStackedBarChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
	this.period = 0;		
}

StackedBar.prototype = new Tile();

StackedBar.prototype.render = function(data, animate) {
    var parent = $("#content-for-" + this.id);

	var canvas = null;
	if(0 == parent.find(".chartcanvas").length) {
		canvas = $("<div>", {class: "chartcanvas"});
		parent.append(canvas);
	}
	else {
		canvas = parent.find(".chartcanvas");
	}
	if(!data.hasOwnProperty("trends")) {
		canvas.empty();
		return;
	}

    console.log(data);
    var colors = new Colors(Object.keys(data.trends).length);
    var d = [];
    var colorIdx = 0;
    for(var trend in data.trends) {
        var rows = [];
        rows.push(['date', 'count']);
        var trendData = data.trends[trend];
        for (var i = trendData.length - 1; i >= 0; i--) {
            rows.push([trendData[i].period, trendData[i].count]);
        };
        d.push({ data: rows, color: colors[colorIdx], label : trend, fill: true, fillColor: colors[colorIdx] });
        colorIdx++;
    }
    var timestamp = new Date().getTime();
    $.plot(canvas, d, {
            series: {
                stack: 0,
                lines: {show: true, fill: 1}
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
            },
            legend: { show:true, position: 'nw', noColumns: 0, labelFormatter: function(label, series){
                return '<font color="black"> &nbsp;' + label +' &nbsp;</font>';
            }}
        });
	/*var colors = new Colors(Object.keys(data.result).length);
	var columns =[];
	for(property in data.result) {
		columns.push({label: property, data: data.result[property], color: colors.nextColor()});
	}
	var chartOptions = {
        series: {
            pie: {
                innerRadius: 0.8,
                show: true,
                label:{
                    show: false
                }
            }
        },
        legend : {
            show: false
        },
        grid: {
        	hoverable: true
        },
        tooltip: true,
        tooltipOpts: {
    		content: function(label, x, y) {
    			return label + ": " + y;
    		}
    	}
    };
    $.plot(canvas, columns, chartOptions);*/

        
	// var chart = c3.generate({
	// 	bindto: chartAreaId,
	// 	size: {
	// 		height: parentHeight,
	// 		width: parentWidth
	// 	},
	// 	data: {
	// 		columns: columns,
	// 		type : 'pie'
	// 	},
	// 	legend: {
	// 		show: false
	// 	},
	// 	transition: {
	// 		duration: transitionTime
	// 	}

	// });

};

StackedBar.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
		return JSON.stringify({
			opcode : "trend",
			table : this.tables.selectedTable.name,
			filters : [{
				field: "_timestamp",
				operator: "between",
				temporal: true,
				from: (timestamp - (this.period * 60000)),
				to: timestamp
			}],
			field : this.eventTypeFieldName,
            period: "minutes"
		});
	}
};

StackedBar.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

StackedBar.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".stacked-bar-chart-field").val();
};

StackedBar.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	var select = modal.find(".stacked-bar-chart-field");
	select.find('option').remove();
	for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
		select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
	};
	if(this.eventTypeFieldName) {
		select.val(this.eventTypeFieldName);
	}
	select.selectpicker('refresh');
	modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");	
}

StackedBar.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
};

StackedBar.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
};