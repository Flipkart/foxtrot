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
function BarTile() {
  this.object = "";
}

function getBarChartFormValues() {
  var period = $("#bar-time-unit").val();
  var timeframe = $("#bar-timeframe").val();
  var eventField = $("#bar-event-field").val();
  var uniqueKey = $("#bar-uniquekey").val();
  var ignoreDigits = $(".bar-ignored-digits").val();
  var selectedValue = $("#bar-selected-value").val();
  var sortingbar =$('.bar-sorting-digits').is(':checked');
  var aggregationType = $('#bar-aggregation-type').val();
  var aggregationField = $('#bar-aggregation-field').val();

  console.log("aggregationType...",aggregationType);
  console.log("aggregationField...",aggregationField);
  console.log("eventField...",eventField);


  console.log("uniqueKey...",$("#bar-uniquekey").val());


  if (eventField == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(eventField)].field;
  var status = true;

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
    , "nesting": [groupingString]
    , "uniqueKey": uniqueKey
    , "ignoreDigits" : ignoreDigits
    , "selectedValue": selectedValue
    , "sortingbar":sortingbar
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
  };
}

function setBarChartFormValues(object) {
  $("#bar-time-unit").val(object.tileContext.period);
  $("#bar-time-unit").selectpicker('refresh');
  $("#bar-timeframe").val(object.tileContext.timeframe);
  $("#bar-event-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#bar-event-field").selectpicker('refresh');
  $("#bar-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
  $("#bar-uniquekey").selectpicker('refresh');
  $(".bar-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  $("#bar-selected-value").val((object.tileContext.selectedValue == undefined ? '' : object.tileContext.selectedValue));
  $('#bar-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#bar-aggregation-field').selectpicker('refresh');
  $('#bar-aggregation-Type').val();

  // ------  start for checkbox set values -------
  if(object.tileContext.sortingbar===true){
    $("#bar-sorting-digits").prop("checked", true);
  }
  else{
    $("#bar-sorting-digits").val((object.tileContext.sortingbar == undefined ? 'undefined' : object.tileContext.sortingbar));
  }
}
  // ------  End for checkbox set values -------


function clearBarChartForm() {
  $('.barForm')[0].reset();
  $(".barForm").find('.selectpicker').selectpicker('refresh');
}

BarTile.prototype.getQuery = function (object) {
  this.object = object;
  var filters = [];
 // -------------- Starts added today yesterday and daybefore yesterday---------------
 todayTomorrow(
  filters,
  globalFilters,
  getGlobalFilters,
  getPeriodSelect,
  timeValue,
  object
);
// -------------- Ends added today yesterday and daybefore yesterday-----------------

  if(object.tileContext.filters) {
    for (var i = 0; i < object.tileContext.filters.length; i++) {
      filters.push(object.tileContext.filters[i]);
    }
  }

  if (object.tileContext.selectedValue) {
    filters.push({
      field: object.tileContext.nesting.toString(),
      operator: "in",
      values: object.tileContext.selectedValue.split(',')
    });
  }

  var templateFilters = isAppendTemplateFilters(object.tileContext.table);
  if(templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }
  
  var listConsole = $("#listConsole").val()
  //var widget = 
  var requestTags = {
    "widget": this.object.title,
    "consoleId":getCurrentConsoleId()
  }
  console.log(" bar console.log", this.object, requestTags);
  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    // ,"aggregationField":object.tileContext.aggregationField && object.tileContext.aggregationField != "none" ? object.tileContext.aggregationField : null
     ,"aggregationField":object.tileContext.aggregationField
    ,"aggregationType":object.tileContext.aggregationType
    ,"sourceType":"ECHO_DASHBOARD"
    ,"requestTags": requestTags
    ,"extrapolationFlag": false
  }
  console.log("data....",data);
  var refObject = this.object;
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
    ,error: function(xhr, textStatus, error) {
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
    }
  });
}
BarTile.prototype.getData = function (data) {
      
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }

  if (data.result == undefined || data.result.length == 0) return;
  var colors = new Colors(Object.keys(data.result).length);
  var columns = [];
  var ticks = [];
  var i = 0;
  this.uniqueValues = [];
  var flatData = [];
  this.object.tileContext.uiFiltersList = [];

  // convert object to array
  var sourceObject = data.result;
  var sortable = [];
  for (var vehicle in sourceObject) {
      sortable.push([vehicle, sourceObject[vehicle]]);
  }

  // ------  start for sorting the values-------
   if (this.object.tileContext.hasOwnProperty('sortingbar')){
    if(this.object.tileContext.sortingbar){
       sortable= customSort(sortable); 
    }
    else{
      sortable.sort(sortFunction);
    }
   }
   else{
    sortable.sort(sortFunction);
   }

  // ------  End for sorting the values-------
  



  function customSort(arr) {
    arr.sort(function(a,b) {
          var descA = a[1];
          var descB = b[1];
          return ((descA > descB) ? -1 : ((descA > descB) ? 1 : 0));
    });
    
    return arr;
 }
  
  // sort by first index
  function sortFunction(a, b) {
    if (a[0] === b[0]) {
      return 0;
    }
    else {
      return (a[0] < b[0]) ? -1 : 1;
    }
  }

  for (var i in sortable) {
    var property = sortable[i][0];
    var valueWithoutRoundOff = sortable[i][1] / Math.pow(10, this.object.tileContext.ignoreDigits);
    var value = valueWithoutRoundOff.toFixed(2);
    var visible = $.inArray( property, this.object.tileContext.uiFiltersSelectedList);
    if((visible == -1 ? true : false)) {
      var dataElement = {
      label: property
      , data: [[i, value]]
      , color: convertHex(colors.nextColor(), 100)
    };
    columns.push(dataElement);
    ticks.push([i, property]);
    flatData.push({
      label: property
      , data: value
      , color: dataElement.color
    });
  }
  this.object.tileContext.uiFiltersList.push(property);
  i++;
}

  var xAxisOptions = {
    tickLength: 0
    , labelWidth: 0
  };
  var tmpLabel = "";
  for (var i = 0; i < ticks.length; i++) {
    tmpLabel += (ticks[i][1] + " ");
  }

  xAxisOptions['ticks'] = null;
  xAxisOptions['tickFormatter'] = function () {
    return "";
  }
  this.render(xAxisOptions, columns);
}
BarTile.prototype.render = function (xAxisOptions, columns) {
  console.log(xAxisOptions, columns)
  if(columns.length == 0) {
    showFetchError(this.object, "data", null)
  }  else {
    hideFetchError(this.object);
  } 
  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  var chartClassName = object.tileContext.widgetSize == undefined ? getFullWidgetClassName(12) : getFullWidgetClassName(object.tileContext.widgetSize);
  ctx.addClass(chartClassName);
  $("#"+object.id).find(".chart-item").find(".legend").addClass('full-widget-legend');
  ctx.height(fullWidgetChartHeight());
  var chartOptions = {
    series: {
      bars: {
        show: true
        , label: {
          show: true
        }
        , barWidth: 0.5
        , align: "center"
        , fill: true
        , fillColor: {
          colors: [{
            opacity: 1
          }, {
            opacity: 1
          }]
        }
      }
      , valueLabels: {
        show: true
      }
    }
    , legend: {
      show: false
    }
    , yaxis: {
      tickLength: 0,
      tickFormatter: function(val, axis) {
        return numDifferentiation(val);
      },
    }
    , xaxis: xAxisOptions,
    grid: {
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
    ,highlightSeries: {
      color: "#FF00FF"
    }
    , tooltip: true
    , tooltipOpts: {
      content: function (label, x, y) {
        return label + ": " + y;
      }
    }
    ,legend: {
      show: false
    }
  };
  var plot = $.plot(ctx, columns, chartOptions);

  if(columns.length == 0) {
    $(ctx).hide();
  } else {
    $(ctx).show();
  }
  
  drawLegend(columns, $(chartDiv.find(".legend")));

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
}


//  -------------------- Starts Added download widget 2 --------------------


BarTile.prototype.downloadWidget = function (object) {
  this.object = object;
  var filters = [];
  // -------------- Starts added today yesterday and daybefore yesterday---------------
  todayTomorrow(
    filters,
    globalFilters,
    getGlobalFilters,
    getPeriodSelect,
    timeValue,
    object
  );
  // -------------- Ends added today yesterday and daybefore yesterday-----------------

  if(object.tileContext.filters) {
    for (var i = 0; i < object.tileContext.filters.length; i++) {
      filters.push(object.tileContext.filters[i]);
    }
  }

  if (object.tileContext.selectedValue) {
    filters.push({
      field: object.tileContext.nesting.toString(),
      operator: "in",
      values: object.tileContext.selectedValue.split(',')
    });
  }

  var templateFilters = isAppendTemplateFilters(object.tileContext.table);
  if(templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }

  var requestTags = {
      "widget": this.object.title,
      "consoleId":getCurrentConsoleId()
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
  var refObject = this.object;
  console.log(data, 'data.')
  $.ajax({
    url: apiUrl + "/v2/analytics/download",
    type: 'POST',
    data: JSON.stringify(data),
    dataType: 'text',

    contentType: 'application/json',
    context: this,
    success: function(response) {
      downloadTextAsCSV(response, 'BarChart.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}

//  -------------------- Ends added download widget 2--------------------


