function DonutTile () {
	this.typeName = "donut";
	this.refresh = true;
	this.setupModalName = "#setupPieChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
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

DonutTile.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

DonutTile.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".pie-chart-field").val();
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
}

DonutTile.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
};

DonutTile.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
};