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
  if (nesting == "none") {
    return [[], false];
  }
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "timeframe": timeframe
    , "period": period
  };
}

function setRadarChartFormValues(object) {
  $("#radar-nesting").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#radar-nesting").selectpicker('refresh');
  $("#radar-timeframe").val(object.tileContext.timeframe);
  $("#radar-time-unit").val(object.tileContext.period);
  $("#radar-time-unit").selectpicker('refresh');
}

function clearRadarChartForm() {
  $('.radarForm')[0].reset();
  $(".radarForm").find('.selectpicker').selectpicker('refresh');
}
RadarTile.prototype.getQuery = function (object) {
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
    "opcode": "group"
    , "table": object.tileContext.table
    , "filters": filters
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
RadarTile.prototype.getData = function (data) {
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
