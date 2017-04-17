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

function StatsTrendTile() {
  this.newDiv = "";
  this.object = "";
}

function getStatsTrendTileChartFormValues() {
  var period = $(".stats-trend-time-unit").val();
  var statsField = $(".stats-trend-field").val();
  var statsToPlot = $(".stats-trend-statics-to-plot").val();
  var timeframe = $("#stats-trend-timeframe").val();

  var status = true;

  if(period == "none" || statsField == "none" || statsToPlot == "none" || timeframe == "") {
    return[[], false];
  }

  if(!$("#stats-trend-time-unit").valid() || !$("#stats-trend-field").valid() || !$("#stats-trend-statics-to-plot").valid || !$("#stats-trend-timeframe").valid) {
    status = false;
  }


  return [{
    "period": period,
    "statsFieldName": currentFieldList[parseInt(statsField)].field,
    "statsToPlot": statsToPlot,
    "timeframe": timeframe
  }, status];
}

function clearStatsTrendTileChartForm() {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find(".stats-trend-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find(".stats-trend-field");
  statsFieldEl.find('option:eq(0)').prop('selected', true);
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find(".stats-trend-statics-to-plot");
  statsToPlot.find('option:eq(0)').prop('selected', true);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#stats-trend-timeframe").val('');
  parentElement.find(".ignored-digits").val('');
}

function setStatsTrendTileChartFormValues(object) {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find(".stats-trend-time-unit");
  timeUnitEl.val(object.tileContext.period);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find(".stats-trend-field");
  var statsFieldIndex = currentFieldList.findIndex(x => x.field== object.tileContext.statsFieldName);
  statsFieldEl.val(statsFieldIndex);
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find(".stats-trend-statics-to-plot");
  statsToPlot.val(object.tileContext.statsToPlot);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#stats-trend-timeframe").val(object.tileContext.timeframe);
  parentElement.find(".ignored-digits").val('');
}

StatsTrendTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)));
  var data = {
    "opcode": "statstrend",
    "table": object.tileContext.table,
    "filters": object.tileContext.filters,
    "field": object.tileContext.statsFieldName,
    "period": periodFromWindow(object.tileContext.period, "custom")
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

StatsTrendTile.prototype.getData = function(data) {
  this.object.tileContext.filters.pop();
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

StatsTrendTile.prototype.render = function (displayValue) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = newDiv.find(".chart-item");
  chartDiv.addClass("trend-chart");

  var a = chartDiv.find("#"+object.id);
  if(a.length != 0) {
    a.remove();
  }

  chartDiv.append("<div id="+object.id+"><p class='trend-value-big bold'>"+displayValue+"</p><dhr/><p class='trend-value-small'></p><div id='trend-'"+object.id+" class='trend-chart-health'></div><div class='trend-chart-health-percentage bold'></div></div>");
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
