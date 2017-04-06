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
  this.newDiv = "";
  this.object = "";
}

function getRadarChartFormValues() {
  var nesting = $(".radar-nesting").val();
  var timeframe = $("#radar-timeframe").val();
  var period = $(".radar-time-unit").val();
  if (nesting == "none") {
    return [[], false];
  }
  var status = true;
  if (!$("#radar-nesting").valid() || !$("#radar-timeframe").valid() || !$("#radar-time-unit").valid()) {
    status = false;
  }
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return [{
    "nesting": nestingArray
    , "timeframe": timeframe
    , "period": period
  }, status];
}

function setRadarChartFormValues(object) {
  $(".radar-nesting").val(currentFieldList.findIndex(x => x.field == object.nesting[0]));
  $(".radar-nesting").selectpicker('refresh');
  $("#radar-timeframe").val(object.timeframe);
  $("#radar-time-unit").val(object.period);
  $("#radar-time-unit").selectpicker('refresh');
}

function clearRadarChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  $("#radar-periodValue").val('');
  var timeUnitEl = parentElement.find("#radar-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  $("#radar-timeframe").val('');
}
RadarTile.prototype.getQuery = function (newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  object.filters.push(timeValue(object.period, object.timeframe, object.periodInterval))
  var data = {
    "opcode": "group"
    , "table": object.table
    , "filters": object.filters
    , "nesting": object.nesting
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
  this.object.filters.pop();
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
  var newDiv = this.newDiv;
  var object = this.object;
  var d = a;
  var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#radar-" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  var mycfg = {
    color: function () {
      c = ['red', 'yellow', 'pink', 'green', 'blue', 'olive', 'aqua', 'cadetblue', 'crimson'];
      m = c.length - 1;
      x = parseInt(Math.random() * 100);
      return c[x % m]; //Get a random color
    }
    , w: 300
    , h: 300
  , }
  RadarChart.draw("#radar-" + object.id, d, mycfg);
}
