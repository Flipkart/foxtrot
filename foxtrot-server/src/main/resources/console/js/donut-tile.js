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

function DonutTile () {
	this.typeName = "donut";
	this.refresh = true;
	this.setupModalName = "#setupPieChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
	this.selectedValues = null;
	this.period = 0;		
}

DonutTile.prototype = new Tile();

DonutTile.prototype.render = function(data, animate) {
	var parent = $("#content-for-" + this.id);

	var chartLabel = null;
	if(0 == parent.find(".pielabel").length) {
		chartLabel = $("<div>", {class: "pielabel"});
		parent.append(chartLabel);
	}
	else {
		chartLabel = parent.find(".pielabel");
	}
	chartLabel.text((this.period >= 60) ? ((this.period / 60) + "h"): (this.period + "m"));

	var canvas = null;
	if(0 == parent.find(".chartcanvas").length) {
		canvas = $("<div>", {class: "chartcanvas"});
		parent.append(canvas);
	}
	else {
		canvas = parent.find(".chartcanvas");
	}
	if(!data.hasOwnProperty("result")) {
		canvas.empty();
		return;
	}

	var colors = new Colors(Object.keys(data.result).length);
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
    $.plot(canvas, columns, chartOptions);
        
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

DonutTile.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
		var filters = [];
		filters.push({
                        field: "_timestamp",
                        operator: "between",
                        temporal: true,
                        from: (timestamp - (this.period * 60000)),
                        to: timestamp
                    });
        if(this.selectedValues) {
            filters.push({
                field: this.eventTypeFieldName,
                operator: "in",
                values: this.selectedValues
            });
        }
		return JSON.stringify({
			opcode : "group",
			table : this.tables.selectedTable.name,
			filters : filters,
			nesting : [this.eventTypeFieldName]
		});
	}
};

DonutTile.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

DonutTile.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".pie-chart-field").val();
	var values = modal.find(".selected-values").val();
	if(values) {
	    this.selectedValues = values.replace(/ /g, "").split(",");
	}
	else {
	    this.selectedValues = null;
	}
};

DonutTile.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	var select = modal.find(".pie-chart-field");
	select.find('option').remove();
	for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
		select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
	};
	if(this.eventTypeFieldName) {
		select.val(this.eventTypeFieldName);
	}
	select.selectpicker('refresh');
	modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");
	if(this.selectedValues) {
        modal.find(".selected-values").val(this.selectedValues.join(", "));
	}
}

DonutTile.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
	representation['selectedValues'] = this.selectedValues;
};

DonutTile.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
	this.selectedValues = representation['selectedValues'];
};