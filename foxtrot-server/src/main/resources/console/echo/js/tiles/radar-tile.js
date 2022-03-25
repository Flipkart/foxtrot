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
function RadarTile() {
  this.object = "";
}

function getRadarChartFormValues() {
  var nesting = $("#radar-nesting").val();
  var timeframe = $("#radar-timeframe").val();
  var period = $("#radar-time-unit").val();
  var aggregationType = $('#radar-aggregation-type').val();
  var aggregationField = $('#radar-aggregation-field').val();
  if (nesting == "none") {
    return [[], false];
  }

  if(aggregationField == "none" || aggregationField == "" || aggregationField == null || aggregationField == "undefined") {
    aggregationField = null;
  } else {
    aggregationField = currentFieldList[parseInt(aggregationField)].field
  }

  if(aggregationType == "none" || aggregationType == "" || aggregationType == "null" || aggregationType == "undefined") {
    aggregationType = null;
  } 

  
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "timeframe": timeframe
    , "period": period
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
  };
}

function setRadarChartFormValues(object) {
  $("#radar-nesting").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#radar-nesting").selectpicker('refresh');
  $("#radar-timeframe").val(object.tileContext.timeframe);
  $("#radar-time-unit").val(object.tileContext.period);
  $("#radar-time-unit").selectpicker('refresh');
  $('#radar-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#radar-aggregation-field').selectpicker('refresh');
  $('#radar-aggregation-Type').val();
}

function clearRadarChartForm() {
  $('.radarForm')[0].reset();
  $(".radarForm").find('.selectpicker').selectpicker('refresh');
}
RadarTile.prototype.getQuery = function (object) {
  this.object = object;
  var filters = [];
  // ------- Starts added   today yesterday and daybefore yesterday---------------
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
    , "nesting": object.tileContext.nesting
    ,"aggregationField":object.tileContext.aggregationField
    ,"aggregationType":object.tileContext.aggregationType
    ,"sourceType":"ECHO_DASHBOARD",
    "requestTags": requestTags,
    "extrapolationFlag": false
  }
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
RadarTile.prototype.getData = function (data) {
  if(data.length == 0)
    showFetchError(this.object);
  else
    hideFetchError(this.object);
    
  this.object.tileContext.filters.pop();
  if (data.result == undefined || data.result.length == 0) return;
  var chartData = [];
  var object = {}
  for (var key in data.result) {
    object.axis = key;
    object.value = data.result[key]
    chartData.push({
      axis: key
      , value: data.result[key]
    });
  }
  this.render(chartData);
}
RadarTile.prototype.render = function (data) {
  var a = [];
  a.push(data);
  var object = this.object;
  var d = a;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#radar-" + object.id);
  ctx.width(ctx.width);
  var mycfg = {
    color: function () {
      c = ['red', 'yellow', 'pink', 'green', 'blue', 'olive', 'aqua', 'cadetblue', 'crimson'];
      m = c.length - 1;
      x = parseInt(Math.random() * 100);
      return c[x % m]; //Get a random color
    }
    , w: 550
    , h: 300
  , }
  RadarChart.draw("#radar-" + object.id, d, mycfg);
}


//  -------------------- Starts added download widget 2--------------------


RadarTile.prototype.downloadWidget = function (object) {
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
    , "nesting": object.tileContext.nesting
    ,"sourceType":"ECHO_DASHBOARD"
    ,"requestTags": requestTags
    ,"extrapolationFlag": false
  }
  var refObject = this.object;
  $.ajax({
    url: apiUrl + "/v2/analytics/download",
    type: 'POST',
    data: JSON.stringify(data),
    dataType: 'text',

    contentType: 'application/json',
    context: this,
    success: function(response) {
      downloadTextAsCSV(response, 'Radar.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}

//  -------------------- Ends added download widget 2--------------------
