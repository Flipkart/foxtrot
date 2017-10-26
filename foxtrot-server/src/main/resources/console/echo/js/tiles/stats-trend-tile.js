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
  this.object = "";
}

function getStatsTrendTileChartFormValues() {
  var period = $("#stats-trend-time-unit").val();
  var statsField = $("#stats-trend-field").val();
  var statsToPlot = $("#stats-trend-statics-to-plot").val();
  var timeframe = $("#stats-trend-timeframe").val();
  var ignoreDigits = $(".stats-trend-ignored-digits").val();
  return {
    "period": period,
    "statsFieldName": currentFieldList[parseInt(statsField)].field,
    "statsToPlot": statsToPlot,
    "timeframe": timeframe
    , "ignoreDigits" : ignoreDigits
  };
}

function clearStatsTrendTileChartForm() {
  $('.statsTrendForm')[0].reset();
  $(".statsTrendForm").find('.selectpicker').selectpicker('refresh');
}

function setStatsTrendTileChartFormValues(object) {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find("#stats-trend-time-unit");
  timeUnitEl.val(object.tileContext.period);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find("#stats-trend-field");
  var statsFieldIndex = currentFieldList.findIndex(x => x.field== object.tileContext.statsFieldName);
  statsFieldEl.val(parseInt(statsFieldIndex));
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find("#stats-trend-statics-to-plot");
  statsToPlot.val(object.tileContext.statsToPlot);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#stats-trend-timeframe").val(object.tileContext.timeframe);
  parentElement.find(".stats-trend-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
}

StatsTrendTile.prototype.getQuery = function(object) {
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
    "opcode": "statstrend",
    "table": object.tileContext.table,
    "filters": filters,
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
  if(!data.result)
    return;

  var results = data.result;
  var selString = "";

  var selectedStats = this.object.tileContext.statsToPlot;
  if( typeof selectedStats === 'string' ) {
    var arr = [];
    arr.push(selectedStats);
    selectedStats = arr;
  }
  var colors = new Colors(selectedStats.length);
  var d = [];
  var colorIdx = 0;
  for (var j = 0; j < selectedStats.length; j++) {
    d.push({
      data: [],
      color: colors.nextColor(),
      label: selectedStats[j],
      lines: {show: true},
      shadowSize: 0/*, curvedLines: {apply: true}*/
    });
  }
  var colorIdx = 0;
  var timestamp = new Date().getTime();
  var tmpData = new Object();
  for (var i = 0; i < results.length; i++) {
    var stats = results[i].stats;
    var percentiles = results[i].percentiles;
    for (var j = 0; j < selectedStats.length; j++) {
      var selected = selectedStats[j];
      var value = 0;
      if (selected.startsWith('percentiles.')) {
        value = percentiles[selected.split("percentiles.")[1]];
      }
      if (selected.startsWith('stats.')) {
        value = stats[selected.split("stats.")[1]];
      }
      d[j].data.push([results[i].period, value / Math.pow(10, this.object.tileContext.ignoreDigits)]);
    }
  }
  this.render(d);
}

StatsTrendTile.prototype.render = function (rows) {
  var object = this.object;
  var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"]
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  var chartClassName = object.tileContext.widgetSize == undefined ? getFullWidgetClassName(12) : getFullWidgetClassName(object.tileContext.widgetSize);
  ctx.addClass(chartClassName);
  $("#"+object.id).find(".chart-item").find(".legend").addClass('full-widget-legend');
  ctx.width(ctx.width - 100);
  ctx.height(fullWidgetChartHeight);
  var plot = $.plot(ctx, rows, {
    series: {
      lines: {
        show: true
        , lineWidth: 1.0
        , color: "#9bc95b"
        , fillColor: {
          colors: [{
            opacity: 1
          }, {
            opacity: 0.25
          }]
        }
        , fill: false
      }
      , points: {
        show: false
      }
      , shadowSize: 0
      , curvedLines: { active: true }
    }
    , xaxis: {
      tickLength: 0
      , mode: "time"
      , timezone: "browser"
      , timeformat: axisTimeFormat(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
      , }
    , yaxis: {
      markingsStyle: 'dashed',
      tickFormatter: function(val, axis) {
        return numDifferentiation(val);
      },
    },
    legend: {
      show: false
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
      , borderColor: "#EEEEEE"
      , }
    , tooltip: false
    , tooltipOpts: {
      content: "%y events at %x"
      , defaultFormat: true
    }
    , });

  drawLegend(rows, $(chartDiv.find(".legend")));

  function showTooltip(x, y, xValue, yValue) {
    var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
    $('<div id="flot-custom-tooltip"> <div class="tooltip-custom-content"><p class="">'+numDifferentiation(yValue)+'</p><p class="tooltip-custom-date-text">' + moment(xValue).format(a) + '</p></div></div>').css( {
      position: 'absolute',
      display: 'none',
      top: y - 60,
      left: x - 2,
    }).appendTo("body").fadeIn(200);
  }

  var previousPoint = null;
  $(ctx).bind("plothover", function (event, pos, item) {
    if (item) {
      if (previousPoint != item.datapoint) {
        previousPoint = item.datapoint;

        $("#flot-custom-tooltip").remove();
        var x = item.datapoint[0].toFixed(0),
            y = item.datapoint[1].toFixed(2);
        showTooltip(item.pageX, item.pageY, Number(x), y);
      }
    } else {
      $("#flot-custom-tooltip").remove();
      clicksYet = false;
      previousPoint = null;
    }
  });
}
