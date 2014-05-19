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