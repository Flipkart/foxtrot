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

function BarTile () {
	this.typeName = "bar";
	this.refresh = true;
	this.setupModalName = "#setupBarChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
    this.selectedValues = null;
	this.period = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uiFilteredValues;

}

BarTile.prototype = new Tile();

BarTile.prototype.render = function(data, animate) {
    if (this.title){
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Group by " + this.eventTypeFieldName);
    }

	var parent = $("#content-for-" + this.id);
	var parentWidth = parent.width();
	var chartLabel = null;
	if(0 == parent.find(".pielabel").length) {
		chartLabel = $("<div>", {class: "pielabel"});
		parent.append(chartLabel);
	}
	else {
		chartLabel = parent.find(".pielabel");
	}
	chartLabel.text(getPeriodString(this.period, $("#" + this.id).find(".period-select").val()));

	var canvas = null;
     var legendArea = null;
     if(this.showLegend) {
         if(0 == parent.find(".chartcanvas").length) {
              canvasParent = $("<div>", {class: "chartcanvas"});
              canvas = $("<div>", {class: "group-chart-area"})
              canvasParent.append(canvas)
              legendArea = $("<div>", {class: "group-legend-area"})
              canvasParent.append(legendArea)
              parent.append(canvasParent);
         }
         else {
              canvas = parent.find(".chartcanvas").find(".group-chart-area");
              legendArea = parent.find(".chartcanvas").find(".group-legend-area");
         }
        var canvasHeight = canvas.height();
        var canvasWidth = canvas.width();
        canvas.width( 0.58 * canvas.parent().width());
        legendArea.width(canvas.parent().width() - canvas.width() - 50);
        chartLabel.width(canvas.width());
        parentWidth = canvasWidth;
        //chartLabel.height(canvas.height());
     }
     else {
       if(0 == parent.find(".chartcanvas").length) {
     		canvas = $("<div>", {class: "chartcanvas"});
     		parent.append(canvas);
     	}
     	else {
     		canvas = parent.find(".chartcanvas");
     	}
     }

	if(!data.hasOwnProperty("result")) {
		canvas.empty();
		return;
	}
	var colors = new Colors(Object.keys(data.result).length);
	var columns =[];
	var ticks = [];
	var i = 0;
	this.uniqueValues = [];
	var flatData = [];
	for(property in data.result) {
	    if(this.isValueVisible(property)) {
	        var dataElement = {label: property, data: [[i, data.result[property]]], color: colors.nextColor()};
    		columns.push(dataElement);
    		ticks.push([i, property]);
    		flatData.push({label: property, data: data.result[property], color: dataElement.color});
	    }
		this.uniqueValues.push(property);
		i++;
	}
    var xAxisOptions = {
                           tickLength: 0,
                           labelWidth: 0
                       };
    var tmpLabel = "";
    for(var i = 0; i < ticks.length; i++) {
        tmpLabel += (ticks[i][1] + " ");
    }
    if(tmpLabel.visualLength() <= parentWidth) {
        xAxisOptions['ticks'] = ticks;
        xAxisOptions['tickFormatter'] = null;
    }
    else {
        xAxisOptions['ticks'] = null;
        xAxisOptions['tickFormatter'] = function() {
            return "";
        }
    }
	var chartOptions = {
        series: {
            bars: {
                show: true,
                label:{
                    show: true
                },
                barWidth: 0.5,
            	align: "center",
            	lineWidth: 1.0,
                fill: true,
                fillColor: { colors: [{ opacity: 0.3 }, { opacity: 0.7}] }
            },
            valueLabels: {
               show: true
            }
        },
        legend : {
            show: false
        },
        xaxis : xAxisOptions/*,
        yaxis: {
        	tickLength: 1,

        }*/,
        /*grid: {
        	hoverable: true,
        	borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
        },*/
        grid: {
                hoverable: true,
                color: "#B2B2B2",
                show: true,
                borderWidth: 1,
                borderColor: "#EEEEEE"
            },
        tooltip: true,
        tooltipOpts: {
    		content: function(label, x, y) {
    			return label + ": " + y;
    		}
    	}
    };
    $.plot(canvas, columns, chartOptions);
    drawLegend(flatData, legendArea);
};

BarTile.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
        var filters = [];
        filters.push(timeValue(this.period, $("#" + this.id).find(".period-select").val()));
        if(this.selectedValues) {
            filters.push({
                field: this.eventTypeFieldName,
                operator: "in",
                values: this.selectedValues
            });
        }
        if(this.selectedFilters && this.selectedFilters.filters){
           for(var i = 0; i<this.selectedFilters.filters.length; i++){
               filters.push(this.selectedFilters.filters[i]);
           }
        }
		return JSON.stringify({
			opcode : "group",
			table : this.tables.selectedTable.name,
			filters : filters,
			nesting : [this.eventTypeFieldName]
		});
	}
};

BarTile.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

BarTile.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".bar-chart-field").val();
    this.title = modal.find(".tile-title").val()
    var values = modal.find(".selected-values").val();
    if(values) {
        this.selectedValues = values.replace(/ /g, "").split(",");
    }
    else {
        this.selectedValues = null;
    }
    var filters = modal.find(".selected-filters").val();
    if(filters != undefined && filters != ""){
        var selectedFilters = JSON.parse(filters);
        if(selectedFilters != undefined){
            this.selectedFilters = selectedFilters;
        }
    }else{
        this.selectedFilters = null;
    }
    this.showLegend = modal.find(".bar-show-legend").prop('checked');
    $("#content-for-" + this.id).find(".chartcanvas").remove();
    $("#content-for-" + this.id).find(".pielabel").remove();
};

BarTile.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
    modal.find(".tile-title").val(this.title)
	var select = modal.find("#bar-chart-field");
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
    if(this.selectedFilters){
       modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".bar-show-legend").prop('checked', this.showLegend);
}

BarTile.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
    representation['selectedValues'] = this.selectedValues;
    representation['showLegend'] = this.showLegend;
    if(this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

BarTile.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
    this.selectedValues = representation['selectedValues'];
    if(representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
    if(representation.hasOwnProperty('showLegend')) {
        this.showLegend = representation['showLegend'];
     }
};

BarTile.prototype.isValueVisible = function(value) {
   return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
}

BarTile.prototype.getUniqueValues = function() {
    var options = [];
    for(var i = 0; i < this.uniqueValues.length; i++) {
        var value = this.uniqueValues[i];
        options.push(
            {
                label: value,
                title: value,
                value: value,
                selected: this.isValueVisible(value)
            }
        );
    }
    return options;
}

BarTile.prototype.filterValues = function(values) {
    if(!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = new Object();
    for(var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
}