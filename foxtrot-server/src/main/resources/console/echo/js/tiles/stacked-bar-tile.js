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
function StackedBarTile() {
  this.newDiv = "";
  this.object = "";
}

function getstackedBarChartFormValues() {
  var period = $(".stacked-bar-time-unit").val();
  var timeframe = $(".stacked-bar-timeframe").val();
  var chartField = $(".stacked-bar-field").val();
  var uniqueKey = $(".stacked-uniquekey").val();
  var ignoreDigits = $(".stackedBar-ignored-digits").val();
  if (chartField == "none") {
    return [[], false];
  }
  chartField = currentFieldList[parseInt(chartField)].field;
  var status = true;
  if (!$("#stacked-bar-time-unit").valid() || !$("#stacked-bar-timeframe").valid()) {
    status = false;
  }
  return [{
    "period": period
    , "timeframe": timeframe
    , "uniqueKey": uniqueKey
    , "stackedBarField": chartField
    , "ignoreDigits" : ignoreDigits
  , }, status]
}

function setStackedBarChartFormValues(object) {
  console.log(object.tileContext);
  $(".stacked-bar-time-unit").val(object.tileContext.period);
  $(".stacked-bar-time-unit").selectpicker('refresh');
  $(".stacked-bar-timeframe").val(object.tileContext.timeframe);
  $(".stacked-bar-field").val(currentFieldList.findIndex(x => x.field == object.tileContext.stackedBarField));
  $(".stacked-bar-field").selectpicker('refresh');
  $(".stacked-bar-uniquekey").val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey));
  $(".stacked-bar-uniquekey").selectpicker('refresh');
  $(".stackedBar-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
}

function clearStackedBarChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  var timeUnitEl = parentElement.find(".stacked-bar-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  var timeframe = parentElement.find(".stacked-bar-timeframe");
  timeframe.val('');
  var stackingKey = parentElement.find(".stacked-bar-field");
  stackingKey.find('option:eq(0)').prop('selected', true);
  $(stackingKey).selectpicker('refresh');
  var stackingBarUniqueKey = parentElement.find(".stacked-bar-uniquekey");
  stackingBarUniqueKey.find('option:eq(0)').prop('selected', true);
  $(stackingBarUniqueKey).selectpicker('refresh');
  $(".stackedBar-ignored-digits").val(0);
}
StackedBarTile.prototype.getQuery = function (newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  if(globalFilters) {
    object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

  var data = {
    "opcode": "trend"
    , "table": object.tileContext.table
    , "filters": object.tileContext.filters
    , "uniqueCountOn": object.tileContext.uniqueCountOn && object.tileContext.uniqueCountOn != "none" ? object.tileContext.uniqueCountOn : null
    , "field": object.tileContext.stackedBarField
    , period: periodFromWindow(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
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
StackedBarTile.prototype.getData = function (data) {
  this.object.tileContext.filters.pop();
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }
  var colors = new Colors(Object.keys(data.trends).length);
  var d = [];
  var colorIdx = 0;
  var timestamp = new Date().getTime();
  var tmpData = new Object();
  var regexp = null;
  for (var trend in data.trends) {
    if (regexp && !regexp.test(trend)) {
      continue;
    }
    var trendData = data.trends[trend];
    for (var i = 0; i < trendData.length; i++) {
      var time = trendData[i].period;
      var count = trendData[i].count / Math.pow(10, (this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits));
      if (!tmpData.hasOwnProperty(time)) {
        tmpData[time] = new Object();
      }
      tmpData[time][trend] = count;
    }
  }
  if (0 == Object.keys(tmpData).length) {
    canvas.empty();
    return;
  }
  var trendWiseData = new Object();
  for (var time in tmpData) {
    for (var trend in data.trends) {
      if (regexp && !regexp.test(trend)) {
        continue;
      }
      var count = 0;
      var timeData = tmpData[time];
      if (timeData.hasOwnProperty(trend)) {
        count = timeData[trend];
      }
      var rows = null;
      if (!trendWiseData.hasOwnProperty(trend)) {
        rows = [];
        trendWiseData[trend] = rows;
      }
      rows = trendWiseData[trend];
      var timeVal = parseInt(time);
      rows.push([timeVal, count]);
    }
  }
  this.object.tileContext.uiFiltersList = [];
  for (var trend in trendWiseData) {
    var rows = trendWiseData[trend];
    if (regexp && !regexp.test(trend)) {
      continue;
    }
    rows.sort(function (lhs, rhs) {
      return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0]) ? 0 : 1);
    })
    var visible = $.inArray( trend, this.object.tileContext.uiFiltersSelectedList);
    if((visible == -1 ? true : false)) {
      d.push({
        data: rows
        , color: colors[colorIdx]
        , label: trend
        , fill: 0.3
        , fillColor: "#A3A3A3"
        , lines: {
          show: true
        }
        , shadowSize: 0 /*, curvedLines: {apply: true}*/
      });
    }
    this.object.tileContext.uiFiltersList.push(trend);
  }
  //console.log(this.object.tileContext.uiFiltersList)
  this.render(d);
}
StackedBarTile.prototype.render = function (d) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = newDiv.find(".chart-item");
  var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"]
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  var plot = $.plot(ctx, d, {
    series: {
      stack: true
      , lines: {
        show: true
        , fill: false
        , lineWidth: 1.0
        , fillColor: {
          colors: [{
            opacity: 1
                    }, {
            opacity: 0.5
                    }]
        }
      },
      points:{show:true}
      , shadowSize: 0
      , curvedLines: { active: true }
    }
    ,crosshair: {
    mode: "x"
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
    }
    , yaxis: {
      tickFormatter: function(val, axis) {
        return numDifferentiation(val);
      },
    }
    , xaxis: {
      mode: "time"
      , timezone: "browser"
      , timeformat: axisTimeFormat(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
      , tickLength: 0
    , }
    , selection: {
      mode: "x"
      , minSize: 1
    }
    , tooltip: false
    , tooltipOpts: {
      content: "%y events at %x"
      , defaultFormat: true
    }
    , legend: {
      show: true
      , noColumns: d.length
      , labelFormatter: function (label, series) {
        return '<span class="legend-custom"> &nbsp;' + label + ' &nbsp;</span>';
      }
      , container: $(chartDiv.find(".legend"))
    }
    ,highlightSeries: {
      color: "#FF00FF"
    }
  });

  function showTooltip(x, y, contents, color) {
    $('<div id="tooltip">' + contents + '</div>').css({
      position: 'absolute',
      display: 'none',
      top: y + 5,
      left: x + 5,
      border: '1px solid #3a4246',
      padding: '2px',
      'background-color': '#425057',
      opacity: 0.80,
      color: "#fff"
    }).appendTo("body").fadeIn(200).fadeOut(60000);
  }

  var previousPoint = null;
  $(ctx).bind("plothover", function (event, pos, item) {
    if (item) {
      $("#tooltip").remove();
      var hoverSeries = item.series; // what series am I hovering?
      var x = item.datapoint[0],
          y = item.datapoint[1];
      var color = item.series.color;
      var strTip = y + " for " + item.series.label; // start string with current hover
      var allSeries = plot.getData();
      $.each(allSeries, function(i,s){ // loop all series
        if (s == hoverSeries) return; // if the loop series is my hover, just keep going
        $.each(s.data, function(j,p){
          if (p[0] == x){  // if my hover x == point x add to string
            strTip += "</br>"+ p[1] + " for " + "<span style="+s.color+">"+s.label+"<span>";
          }
          else {
            $("#tooltip").remove();
            previousPoint = null;
          }
        });
      });
      showTooltip(item.pageX, item.pageY, strTip, color);
    } else {
      $("#tooltip").remove();
    }
  });

  $('.legend .legendLabel, .legend .legendColorBox').on('mouseenter', function() {
    var label = $(this).text();
    var allSeries = plot.getData();
    for (var i = 0; i < allSeries.length; i++){
      if (allSeries[i].label == $.trim(label)){
        allSeries[i].oldColor = allSeries[i].color;
        allSeries[i].color = 'black';
        break;
      }
    }
    plot  .draw();
  });

  $('.legend .legendLabel, .legend .legendColorBox').on('mouseleave', function() {
    var label = $(this).text();
    var allSeries = plot.getData();
    for (var i = 0; i < allSeries.length; i++){
      if (allSeries[i].label == $.trim(label)){
        allSeries[i].color = allSeries[i].oldColor;
        break;
      }
    }
    plot.draw();
  });
}
