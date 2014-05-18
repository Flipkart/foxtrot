function LineTile () {
	this.query = JSON.stringify({
		opcode : "group",
		table : "abcd",
		nesting : [ "header.configName", "data.name"]
	});
	this.refresh = true;	
}

LineTile.prototype = new Tile();

LineTile.prototype.render = function(data, animate) {
	var chartAreaId = "#content-for-" + this.id;
	var chartContent = $("#" + this.id).find(chartAreaId);
	var parentHeight = chartContent.parent().height();
	
	var parentWidth = chartContent.parent().width();
	transitionTime = (animate) ? 100:0;
	var chart = c3.generate({
		bindto: chartAreaId,
		size: {
			height: parentHeight,
			width: parentWidth
		},
		data: {
			columns: [
	            ['data1', 30, 200, 100, 400, 150, 250],
	            ['data2', 50, 20, 10, 40, 15, 25]
	        ],
			type : 'line'
		},
		legend: {
			show: false
		},
		transition: {
			duration: transitionTime
		}

	});

};