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
function StackedTile() {
  this.newDiv = "";
  this.object = "";
}

function getstackedChartFormValues() {
  var period = $(".stacked-time-unit").val();
  var timeframe = $(".stacked-timeframe").val();
  var groupingKey = $(".stacked-grouping-key").val();
  var stackingKey = $(".stacking-key").val();
  var uniqueKey = $(".stacked-uniquekey").val();
  if (groupingKey == "none" || stackingKey == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(groupingKey)].field;
  var stackingString = currentFieldList[parseInt(stackingKey)].field;
  var nestingArray = [];
  nestingArray.push(groupingString);
  nestingArray.push(stackingString);
  var status = true;
  if (!$("#stacked-time-unit").valid() || !$("#stacked-timeframe").valid()) {
    status = false;
  }
  return [{
    "period": period
    , "timeframe": timeframe
    , "groupingKey": groupingString
    , "stackingKey": stackingString
    , "uniqueKey": uniqueKey
    , "nesting": nestingArray
  }, status]
}

function setStackedChartFormValues(object) {
  $(".stacked-time-unit").val(object.tileContext.period);
  $("stacked-time-unit").selectpicker('refresh');
  $(".stacked-timeframe").val(object.tileContext.timeframe);
  $(".stacked-grouping-key").val(currentFieldList.findIndex(x => x.field == object.tileContext.groupingKey));
  $(".stacked-grouping-key").selectpicker('refresh');
  $(".stacking-key").val(currentFieldList.findIndex(x => x.field == object.tileContext.stackingKey));
  $(".stacking-key").selectpicker('refresh');
  $(".stacked-uniquekey").val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey));
  $(".stacked-uniquekey").selectpicker('refresh');
}

function clearstackedChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  var timeUnitEl = parentElement.find(".stacked-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  var timeframe = parentElement.find(".stacked-timeframe");
  timeframe.val('');
  var groupingKey = parentElement.find(".stacked-grouping-key");
  groupingKey.find('option:eq(0)').prop('selected', true);
  $(groupingKey).selectpicker('refresh');
  var stackingKey = parentElement.find(".stacking-key");
  stackingKey.find('option:eq(0)').prop('selected', true);
  $(stackingKey).selectpicker('refresh');
  var stackingBarUniqueKey = parentElement.find(".stacked-uniquekey");
  stackingBarUniqueKey.find('option:eq(0)').prop('selected', true);
  $(stackingBarUniqueKey).selectpicker('refresh');
}
StackedTile.prototype.getQuery = function (newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  this.object.tileContext.filters.pop();
  if(globalFilters) {
    object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }
  var data = {
    "opcode": "group"
    , "table": object.tileContext.table
    , "filters": object.tileContext.filters
    , "uniqueCountOn": object.tileContext.uniqueCountOn && object.tileContext.uniqueCountOn != "none" ? object.tileContext.uniqueCountOn : null
    , "nesting": object.tileContext.nesting
  }
  $.ajax({
    method: "post"
    , dataType: 'json'
    , accepts: {
      json: 'application/json'
    }
    , url: apiUrl + "/v1/analytics"
    , contentType: "application/json"
    , data: JSON.stringify(data)
    , success: $.proxy(this.getData, this)
  });
}

function unique(list) {
  var result = [];
  $.each(list, function (i, e) {
    if ($.inArray(e, result) == -1) result.push(e);
  });
  return result;
}
StackedTile.prototype.getData = function (data) {
  this.object.tileContext.filters.pop();
  if (data.result == undefined || data.result.length == 0) return;
  var xAxis = [];
  var yAxis = [];
  var label = [];
  var i = 0;
  var queryResult = data.result;

  // First Get unique x-axis values and define x-axis index for them
  var xAxisTicks = [];
  var xAxisTicksMap = {};
  var index = 0;
  for (var xAxisKey in queryResult) {
    if (!queryResult.hasOwnProperty(xAxisKey)) {
      continue;
    }
    xAxisTicks.push([index, xAxisKey]);
    xAxisTicksMap[xAxisKey] = index;
    index += 1;
  }

  // Now calculate all possible y axis values
  var yAxisTicks = {};
  var yAxisSeriesMap = {};
  index = 0;
  for (xAxisKey in queryResult) {
    if (!queryResult.hasOwnProperty(xAxisKey)) {
      continue;
    }

    for (var yAxisKey in queryResult[xAxisKey]) {
      if (!queryResult[xAxisKey].hasOwnProperty(yAxisKey)) {
        continue;
      }
      if (!yAxisTicks.hasOwnProperty(yAxisKey)) {
        yAxisTicks[yAxisKey] = index;
        yAxisSeriesMap[yAxisKey] = [];
        index += 1;
      }
    }
  }


  // Now define y-axis series data
  for (xAxisKey in queryResult) {
    if (!queryResult.hasOwnProperty(xAxisKey)) {
      continue;
    }
    var xAxisKeyData = queryResult[xAxisKey];
    for (yAxisKey in yAxisSeriesMap) {
      if (!yAxisSeriesMap.hasOwnProperty(yAxisKey)) {
        continue;
      }

      if (xAxisKeyData.hasOwnProperty(yAxisKey)) {
        yAxisSeriesMap[yAxisKey].push([xAxisTicksMap[xAxisKey], xAxisKeyData[yAxisKey]])
      } else {
        yAxisSeriesMap[yAxisKey].push([xAxisTicksMap[xAxisKey], 0])
      }


    }
  }
  var yAxisSeries = [];
  for (var yAxisSeriesElement in yAxisSeriesMap) {
    if (!yAxisSeriesMap.hasOwnProperty(yAxisSeriesElement)) {
      continue;
    }
    if (yAxisSeriesMap[yAxisSeriesElement].length > 0) {
      yAxisSeries.push({label: yAxisSeriesElement, data: yAxisSeriesMap[yAxisSeriesElement]})
    }
  }
  this.render(yAxisSeries, xAxisTicks)
}
StackedTile.prototype.render = function (yAxisSeries, xAxisTicks) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  ctx.addClass('col-sm-10');
  $("#"+object.id).find(".chart-item").find(".legend").addClass('full-widget-legend');
  $.plot(ctx, yAxisSeries, {
    series: {
      stack: true
      , bars: {
        show: true,
        label: {
          show: true
        },
        barWidth: 0.5,
        align: "center",
        lineWidth: 1.0,
        fill: true,
        fillColor: {colors: [{opacity: 1}, {opacity: 1}]}
      }
    }
    , grid: {
      hoverable: true
      , color: "#B2B2B2"
      , show: true
      , borderWidth: {
        top: 0
        , right: 0
        , bottom: 1
        , left: 1
      }
    }
    , xaxis: {
      ticks: xAxisTicks,
      tickLength: 0
    }
    , yaxis:{
      tickLength: 0,
      tickFormatter: function(val, axis) {
        return numDifferentiation(val);
      },
     }
    , selection: {
      mode: "x"
      , minSize: 1
    }
    , tooltip: true
    , tooltipOpts: {
      content:
        "%s: %y events at %x"
      , defaultFormat: true
    }
    ,legend: {
      show: false
      , noColumns: getLegendColumn(object.tileContext.widgetType)
      , labelFormatter: function (label, series) {
        return '<span class="legend-custom"> &nbsp;' + label + ' &nbsp;</span>';
      }
      , container: $(chartDiv.find(".legend"))
    }
  });
  drawLegend(yAxisSeries, $(chartDiv.find(".legend")));
}
