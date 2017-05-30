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
  this.object = "";
}

function getTrendChartFormValues() {
  var period = $("#trend-time-unit").val();
  var statsField = $("#stats-field").val();
  var statsToPlot = $("#statistic_to_plot").val();
  var timeframe = $("#trend-timeframe").val();
  var ignoreDigits = $(".trend-ignored-digits").val();

  var status = true;

  if(period == "none" || statsField == "none" || statsToPlot == "none" || timeframe == "") {
    return[[], false];
  }

  if(!$("#trend-time-unit").valid() || !$("#stats-field").valid() || !$("#statistic_to_plot").valid || !$("#trend-timeframe").valid) {
    status = false;
  }


  return [{
    "period": period,
    "statsFieldName": currentFieldList[parseInt(statsField)].field,
    "statsToPlot": statsToPlot,
    "timeframe": timeframe
    , "ignoreDigits" : ignoreDigits
  }, status];
}

function clearTrendChartForm() {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find("#trend-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find("#stats-field");
  statsFieldEl.find('option:eq(0)').prop('selected', true);
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find("#statistic_to_plot");
  statsToPlot.find('option:eq(0)').prop('selected', true);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#trend-timeframe").val('');
  parentElement.find("#trend-ignored-digits").val(0);
}

function setTrendChartFormValues(object) {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find("#trend-time-unit");
  timeUnitEl.val(object.tileContext.period);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find("#stats-field");
  var statsFieldIndex = currentFieldList.findIndex(x => x.field== object.tileContext.statsFieldName);
  statsFieldEl.val(parseInt(statsFieldIndex));
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find("#statistic_to_plot");
  statsToPlot.val(object.tileContext.statsToPlot);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#trend-timeframe").val(object.tileContext.timeframe);
  parentElement.find("#trend-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
}

TrendTile.prototype.getQuery = function(object) {
  this.object = object;
  var filters = [];
  if(globalFilters) {
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

  if(object.tileContext.filters) {
    for (var i = 0; i < object.tileContext.filters.length; i++) {
      filters.push(object.tileContext.filters[i]);
    }
  }

  var data = {
    "opcode": "stats",
    "table": object.tileContext.table,
    "filters": filters,
    "field": object.tileContext.statsFieldName
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

TrendTile.prototype.getData = function(data) {
  if(!data.result)
    return;
  var statsObject = data.result.stats;
  var percentile = data.result.percentiles;
  var displayValue = "";
  var objectToshow = this.object.tileContext.statsToPlot.split('.');
  if(this.object.tileContext.statsToPlot.match('stats')) {
    objectToshow = objectToshow[1].toString();
    displayValue = statsObject[objectToshow];
  } else {
    var displayObject = objectToshow[1]+'.'+objectToshow[2].toString();
    displayValue = percentile[displayObject];
  }
  this.render(displayValue);
}

TrendTile.prototype.render = function (displayValue) {
  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("trend-chart");

  var a = chartDiv.find("#"+object.id);
  if(a.length != 0) {
    a.remove();
  }

  displayValue = displayValue / Math.pow(10, (this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits));
  chartDiv.append("<div id="+object.id+"><p class='trend-value-big bold'>"+numberWithCommas(displayValue)+"</p><dhr/><p class='trend-value-small'></p><div id='trend-'"+object.id+" class='trend-chart-health'></div><div class='trend-chart-health-percentage bold'></div></div>");
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
