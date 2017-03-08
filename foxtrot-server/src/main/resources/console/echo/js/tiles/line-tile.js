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

function LineTile() {

}

LineTile.prototype.render = function (newDiv, object) {
	var min = 100;
	var max = 10000;
// and the formula is:
	var chartObject = {
        "8": Math.floor(Math.random() * (max - min + 1)) + min,
        "9": Math.floor(Math.random() * (max - min + 1)) + min,
        "lollypop": Math.floor(Math.random() * (max - min + 1)) + min,
        "ics": Math.floor(Math.random() * (max - min + 1)) + min,
        "marshmallow": Math.floor(Math.random() * (max - min + 1)) + min,
        "kitkat": Math.floor(Math.random() * (max - min + 1)) + min,
        "jellybean": Math.floor(Math.random() * (max - min + 1)) + min
    };

		var yValue = [];
		var xValue = [];
		var index = 0;
		for (var key in chartObject) {
			if (chartObject.hasOwnProperty(key)) {
				yValue.push([index, chartObject[key]]);
				xValue.push([index, key])
			}
			index++;
		}

		var chartDiv = newDiv.find(".chart-item");
		var ctx = chartDiv.find("#"+object.id);
		ctx.width(ctx.width);
		ctx.height(230);
		$.plot(ctx, [
			{ data: yValue },
		], {
			series: {
				lines: { show: true, lineWidth: 3.0, color: "#000"},
				points: { show: false }
			},
			xaxis: {
				ticks: xValue,
				tickLength:0
			},
			grid: {
				hoverable: true,
        color: "#B2B2B2",
        show: true,
        borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
        borderColor: "#EEEEEE",
			},
			tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        },
			colors: ['#000'],
		});

		var healthDiv = chartDiv.find("#"+object.id+"-health");
		healthDiv.width(100);
		healthDiv.height(50);
		$.plot(healthDiv, [
			{ data: yValue },
		], {
			series: {
				lines: { show: true },
				points: { show: false }
			},
			xaxis: {
				ticks: xValue,
				tickLength:0
			},
			grid: {
				hoverable: true,
        color: "#B2B2B2",
        show: false,
        borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
        borderColor: "#EEEEEE",
			},
			tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        },
			colors: ['#000'],
		});
}
