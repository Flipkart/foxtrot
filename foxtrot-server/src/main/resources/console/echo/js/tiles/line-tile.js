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

function getLineChartFormValues() {
  var period = $("#tile-time-unit").val();
  var uniqueCount = $("#uniqueKey").val();
  var timeframe = $("#line-timeframe").val();

  var status = false;
  if($("#uniqueKey").valid() && $("#tile-time-unit").valid() && $("#line-timeframe").valid()) {
    status = true;
  }

  return [{
    "period": period,
    "uniqueCountOn": uniqueCount,
    "timeframe": timeframe,
  }, status]
}

function setLineChartFormValues(object) {
  var parentElement = $("#"+object.chartType+"-chart-data");

  var timeUnitEl = parentElement.find(".tile-time-unit");
  timeUnitEl.val(object.period);
  $(timeUnitEl).selectpicker('refresh');

  var uniqeKey = parentElement.find("#uniqueKey");
  uniqeKey.val(object.uniqueCountOn);
  $(uniqeKey).selectpicker('refresh');

  parentElement.find("#line-timeframe").val(object.timeframe);
}

function clearLineChartForm () {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find(".tile-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');

  var uniqeKey = parentElement.find("#uniqueKey");
  uniqeKey.find('option:eq(0)').prop('selected', true);
  $(uniqeKey).selectpicker('refresh');

  parentElement.find("#line-timeframe").val('');
}

LineTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var ts = new Date().getTime();
  var duration = object.timeframe+object.period;
  object.filters.push( {field: "_timestamp", operator: "last", duration: duration, currentTime: ts})
  var data = {
    "opcode": "histogram",
    "table": object.table,
    "filters": object.filters,
    "field": "_timestamp",
    "period": object.period,
    "uniqueCountOn": object.uniqueCountOn && object.uniqueCountOn != "none" ? object.uniqueCountOn : null
  }
  $.ajax({
    method: "post",
    dataType: 'json',
    accepts: {
        json: 'application/json'
    },
    url: apiUrl+"/v1/analytics",
    contentType: "application/json",
    data: JSON.stringify(data),
    success: $.proxy(this.getData, this)
  });
}

LineTile.prototype.getData = function(data) {
  this.object.filters.pop();
  if(data.counts == undefined || data.counts.length == 0)
    return;
  var rows = [];
  rows.push(['date', 'count']);
  for (var i = data.counts.length - 1; i >= 0; i--) {
    rows.push([data.counts[i].period, data.counts[i].count]);
  }
  this.render(rows);
}

LineTile.prototype.render = function (rows) {
  var newDiv = this.newDiv;
  var object = this.object;
	var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#"+object.id);
	ctx.width(ctx.width - 100);
	ctx.height(230);
	$.plot(ctx, [
		{ data: rows },
  ], {
    series: {
		lines: { show: true, lineWidth: 4.0, color: "#9bc95b"},
		points: { show: false }
		},
		xaxis: {
		tickLength:0,
    mode: "time",
    timezone: "browser",
    timeformat: axisTimeFormat(object.period, "custom"),
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
    colors: ['#50e3c2'],
  });

  var healthParentDiv = newDiv.find(".widget-header")
  var healthDiv = healthParentDiv.find("#"+object.id+"-health");
	healthDiv.width(100);
	healthDiv.height(50);
	$.plot(healthDiv, [
		{ data: rows },
  ],{
			series: {
				lines: { show: true },
				points: { show: false }
			},
			xaxis: {
				tickLength:0,
        mode: "time",
        timezone: "browser",
        timeformat: axisTimeFormat(object.period, "custom"),
			},
			grid: {
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
