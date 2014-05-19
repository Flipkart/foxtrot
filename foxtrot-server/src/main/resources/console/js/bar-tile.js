function BarTile () {
	this.typeName = "bar";
	this.refresh = true;
	this.setupModalName = "#setupBarChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
	this.period = 0;		
}

BarTile.prototype = new Tile();

BarTile.prototype.render = function(data, animate) {
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
	var ticks = [];
	var i = 0;
	for(property in data.result) {
		columns.push({label: property, data: [[i, data.result[property]]], color: colors.nextColor()});
		ticks.push([i, property]);
		i++;
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
            	lineWidth: 0,
            	fill: 1.0
            }
        },
        legend : {
            show: false
        },
        xaxis : {
        	ticks: ticks,
        	tickLength: 0        	
        },
        yaxis: {
        	tickLength: 0
        },
        grid: {
        	hoverable: true,
        	borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
        },
        tooltip: true,
        tooltipOpts: {
    		content: function(label, x, y) {
    			return label + ": " + y;
    		}
    	}
    };
    $.plot(canvas, columns, chartOptions);
};

BarTile.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
		return JSON.stringify({
			opcode : "group",
			table : this.tables.selectedTable.name,
			filters : [{
				field: "_timestamp",
				operator: "between",
				temporal: true,
				from: (timestamp - (this.period * 60000)),
				to: timestamp
			}],
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
};

BarTile.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	var select = modal.find(".bar-chart-field");
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

BarTile.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
};

BarTile.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
};