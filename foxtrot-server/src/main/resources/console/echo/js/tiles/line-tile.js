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
  this.newDiv = "";
  this.object = "";
}

LineTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var data = {
    "opcode": "histogram",
    "table": object.table,
    "filters": object.filters,
    "field": "_timestamp",
    "period": object.period
  }
  $.ajax({
    method: "post",
    dataType: 'json',
    accepts: {
        json: 'application/json'
    },
    url: "http://foxtrot.traefik.prod.phonepe.com/foxtrot/v1/analytics",
    contentType: "application/json",
    data: JSON.stringify(data),
    success: $.proxy(this.getData, this)
  });
}

function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;

    return [year, month, day].join('-');
}

LineTile.prototype.getData = function(data) {
  var xAxis = [];
  var yAxis = [];
  for(var i = 0; i< data.counts.length; i++) {
    var date = new Date(data.counts[i].period);
    xAxis.push([i, formatDate(date)]);
    yAxis.push([i, data.counts[i].count ]);
  }
  this.render(xAxis, yAxis);
}

LineTile.prototype.render = function (xAxis, yAxis) {
  var newDiv = this.newDiv;
  var object = this.object;
	var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#"+object.id);
	ctx.width(ctx.width);
	ctx.height(230);
	$.plot(ctx, [
		{ data: yAxis },
  ], {
    series: {
		lines: { show: true, lineWidth: 3.0, color: "#000"},
		points: { show: false }
		},
		xaxis: {
		ticks: xAxis,
		tickLength:0
    },
    yaxis: {
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
		{ data: yAxis },
  ],{
			series: {
				lines: { show: true },
				points: { show: false }
			},
			xaxis: {
				ticks: xAxis,
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
