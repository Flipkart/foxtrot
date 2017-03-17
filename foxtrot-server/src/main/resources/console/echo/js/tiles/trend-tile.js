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

function TrendTile() {
  this.newDiv = "";
  this.object = "";
}

TrendTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var data = {
    "opcode": "stats",
    "table": object.table,
    "filters": object.filters,
    "field": object.statsFieldName
  }
  console.log(data);
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

TrendTile.prototype.getData = function(data) {
  if(!data.result)
    return;
  var statsObject = data.result.stats;
  var percentile = data.result.percentiles;
  var displayValue = "";
  var objectToshow = this.object.statsToPlot.split('.');
  if(this.object.statsToPlot.match('stats')) {
    objectToshow = objectToshow[1].toString();
    console.log(objectToshow);
    displayValue = statsObject[objectToshow];
  } else {
    var displayObject = objectToshow[1]+'.'+objectToshow[2].toString();
    displayValue = percentile[displayObject];
  }
  this.render(displayValue);
}

TrendTile.prototype.render = function (displayValue) {
  var newDiv = this.newDiv;
  var object = this.object;
  //var d = [data];
  var chartDiv = newDiv.find(".chart-item");
  chartDiv.addClass("trend-chart");
  chartDiv.append("<div><p class='trend-value-big'>"+displayValue+"</p><hr/><p class='trend-value-small'>15 Million</div>");
  chartDiv.append('<div id="trend-'+object.id+'" class="trend-chart-health"></div>')
  chartDiv.append('<div class="trend-chart-health-percentage">6%</div>')

  var healthDiv = chartDiv.find("#trend-"+object.id);
	healthDiv.width(100);
	healthDiv.height(50);
	/*$.plot(healthDiv, [
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
		});*/


}
