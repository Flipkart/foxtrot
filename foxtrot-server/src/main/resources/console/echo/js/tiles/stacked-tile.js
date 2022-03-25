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
  this.object = "";
}

function getstackedChartFormValues() {
  var period = $("#stacked-time-unit").val();
  var timeframe = $("#stacked-timeframe").val();
  var groupingKey = $("#stacked-grouping-key").val();
  var stackingKey = $("#stacking-key").val();
  var uniqueKey = $("#stacked-uniquekey").val();
  var aggregationType = $('#stacked-aggregation-type').val();
  var aggregationField = $('#stacked-aggregation-field').val();

  
  if (groupingKey == "none" || stackingKey == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(groupingKey)].field;
  var stackingString = currentFieldList[parseInt(stackingKey)].field;
  var nestingArray = [];
  nestingArray.push(groupingString);
  nestingArray.push(stackingString);

  if(uniqueKey == "none" || uniqueKey == "" || uniqueKey == null) {
    uniqueKey = null;
  } else {
    uniqueKey = currentFieldList[parseInt(uniqueKey)].field
  }


  if(aggregationField == "none" || aggregationField == "" || aggregationField == null || aggregationField == "undefined") {
    aggregationField = null;
  } else {
    aggregationField = currentFieldList[parseInt(aggregationField)].field
  }

  if(aggregationType == "none" || aggregationType == "" || aggregationType == "null" || aggregationType == "undefined") {
    aggregationType = null;
  } 
  
  return {
    "period": period
    , "timeframe": timeframe
    , "groupingKey": groupingString
    , "stackingKey": stackingString
    , "uniqueKey": uniqueKey
    , "nesting": nestingArray
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
  };
}

function setStackedChartFormValues(object) {
  $("#stacked-time-unit").val(object.tileContext.period);
  $("#stacked-time-unit").selectpicker('refresh');
  $("#stacked-timeframe").val(object.tileContext.timeframe);
  $("#stacked-grouping-key").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.groupingKey)));
  $("#stacked-grouping-key").selectpicker('refresh');
  $("#stacking-key").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.stackingKey)));
  $("#stacking-key").selectpicker('refresh');
  $("#stacked-uniquekey").val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey));
  $("#stacked-uniquekey").selectpicker('refresh');
  $('#stacked-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#stacked-aggregation-field').selectpicker('refresh');
  $('#stacked-aggregation-Type').val();
}

function clearstackedChartForm() {
  $('.stackedForm')[0].reset();
  $(".stackedForm").find('.selectpicker').selectpicker('refresh');
}
StackedTile.prototype.getQuery = function (object) {
  this.object = object;
  var filters = [];
// ------- Starts added  download for today yesterday and daybefore yesterday---------------
 todayTomorrow(
  filters,
  globalFilters,
  getGlobalFilters,
  getPeriodSelect,
  timeValue,
  object
);
// ------ Ends added today yesterday and daybefore yesterday-------------------------------

  if(object.tileContext.filters) {
    for (var i = 0; i < object.tileContext.filters.length; i++) {
      filters.push(object.tileContext.filters[i]);
    }
  }

  var templateFilters = isAppendTemplateFilters(object.tileContext.table);
  if(templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }
  
  var requestTags = {
    "widget": this.object.title,
    "consoleId": getCurrentConsoleId()
  }
  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    ,"aggregationField":object.tileContext.aggregationField
    ,"aggregationType":object.tileContext.aggregationType
    ,"sourceType":"ECHO_DASHBOARD",
    "requestTags": requestTags,
    "extrapolationFlag": false
  }
  var currentTileId = this.object.id;

  var refObject = this.object;
  $.ajax({
    method: "post"
    , dataType: 'json'
    , accepts: {
      json: 'application/json'
    }
    , url: apiUrl + "/v2/analytics"
    , contentType: "application/json"
    , data: JSON.stringify(data)
    , success: $.proxy(this.getData, this)
    ,error: function(xhr, textStatus, error) {
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
    }
  });
}

StackedTile.prototype.getData = function (data) {
    
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
  var colors = new Colors(Object.keys(yAxisSeriesMap).length);
  this.object.tileContext.uiFiltersList = [];
  for (var yAxisSeriesElement in yAxisSeriesMap) {
    if (!yAxisSeriesMap.hasOwnProperty(yAxisSeriesElement)) {
      continue;
    }
    if (yAxisSeriesMap[yAxisSeriesElement].length > 0) {
      var visible = $.inArray( yAxisSeriesElement, this.object.tileContext.uiFiltersSelectedList);
      if((visible == -1 ? true : false)) {
        yAxisSeries.push({label: yAxisSeriesElement, data: yAxisSeriesMap[yAxisSeriesElement], color:convertHex(colors.nextColor(), 100)})
      }
      this.object.tileContext.uiFiltersList.push(yAxisSeriesElement);
    }
  }
  this.render(yAxisSeries, xAxisTicks)
}
StackedTile.prototype.render = function (yAxisSeries, xAxisTicks) {

  if(xAxisTicks.length == 0) {
    showFetchError(this.object, "data", null);
  } else {
    hideFetchError(this.object);
  }

  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(fullWidgetChartHeight());
  var chartClassName = object.tileContext.widgetSize == undefined ? getFullWidgetClassName(12) : getFullWidgetClassName(object.tileContext.widgetSize);
  ctx.addClass(chartClassName);
  $("#"+object.id).find(".chart-item").find(".legend").addClass('full-widget-legend');
  var plot = $.plot(ctx, yAxisSeries, {
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
    }
  });

  drawLegend(yAxisSeries, $(chartDiv.find(".legend")));

  var re = re = /\(([0-9]+,[0-9]+,[0-9]+)/;
  $(chartDiv.find('.legend ul li')).on('mouseenter', function() {
    var label = $(this).text();
    var allSeries = plot.getData();
    for (var i = 0; i < allSeries.length; i++){
      if (allSeries[i].label == $.trim(label)){
        allSeries[i].oldColor = allSeries[i].color;
        allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 1 + ')';
      } else {
        allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 0.1 + ')';
      }
    }
    plot.draw();
  });

  $(chartDiv.find('.legend ul li')).on('mouseleave', function() {
    var label = $(this).text();
    var allSeries = plot.getData();
    for (var i = 0; i < allSeries.length; i++){
      allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 1 + ')';
    }
    plot.draw();
  });

  displayBarValues();

  // display values on top of bars
  function displayBarValues() {
    var plotData = plot.getData();
    var xValueToStackValueMapping = [];

    // loop through each data series
    for (var i = 0; i < plotData.length; i++) {
      var series = plotData[i];

      // loop through each data point in the series
      for (var j = 0; j < series.data.length; j++) {
        var value = series.data[j];

        // if the x axis value is not in the mapping, add it.
        if (!xValueExistsInMapping(xValueToStackValueMapping, value[0])) {
          xValueToStackValueMapping.push([value[0], 0]);
        }

        // add the value of the bar to the x value mapping
        addValueToMapping(xValueToStackValueMapping, value[0], value[1]);
      }
    }

    // loop through each of our stacked values and place them on the bar chart
    $.each(xValueToStackValueMapping, function(i, value) {
      // find the offset of the top left of the bar
      var leftoffset = plot.pointOffset({ x: value[0] - .5, y: value[1] });

      // find the offset of the top right of the bar (our bar width is .5)
      var rightoffset = plot.pointOffset({ x: value[0] + .5, y: value[1] });

      $('<div class="data-point-value">' + numberWithCommas(value[1]) + '</div>').css({
        left: leftoffset.left,
        top: leftoffset.top - 25,
        width: rightoffset.left - leftoffset.left,
        textAlign: 'center',
        'font-size': '14px',
        'font-family': "Oswald Light"
      }).appendTo(plot.getPlaceholder());
    });


  }

  function xValueExistsInMapping(mapping, value) {
    for (var i = 0; i < mapping.length; i++) {
      if (mapping[i][0] !== undefined && mapping[i][0] === value) {
        return true;
      }
    }
    return false;
  }

  function addValueToMapping(mapping, xValue, yValue) {
    for (var i = 0; i < mapping.length; i++) {
      if (mapping[i][0] === xValue) {
        mapping[i][1] = mapping[i][1] + yValue;
      }
    }
  }

  if(xAxisTicks.length == 0) {
    ctx.hide();
  } else {
    ctx.show();
  }
}



//  -------------------- Start added download widget 2--------------------


StackedTile.prototype.downloadWidget= function (object) {
  this.object = object;
  var filters = [];
// ------- Starts added  download for today yesterday and daybefore yesterday---------------
 todayTomorrow(
  filters,
  globalFilters,
  getGlobalFilters,
  getPeriodSelect,
  timeValue,
  object
);
// ------ Ends added today yesterday and daybefore yesterday-------------------------------

  if(object.tileContext.filters) {
    for (var i = 0; i < object.tileContext.filters.length; i++) {
      filters.push(object.tileContext.filters[i]);
    }
  }

  var templateFilters = isAppendTemplateFilters(object.tileContext.table);
  if(templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }

  var requestTags = {
    "widget": this.object.title,
    "consoleId": getCurrentConsoleId()
  }
  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    ,"sourceType":"ECHO_DASHBOARD"
    ,"requestTags": requestTags
    ,"extrapolationFlag": false
  }
  var currentTileId = this.object.id;
  var refObject = this.object;
  $.ajax({
    url: apiUrl + "/v2/analytics/download",
    type: 'POST',
    data: JSON.stringify(data),
    dataType: 'text',

    contentType: 'application/json',
    context: this,
    success: function(response) {
      downloadTextAsCSV(response, 'StackedBarChart.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}

//  -------------------- Ends added download widget 2--------------------
