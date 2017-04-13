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
  object.tileContext.filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
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
  for (var key in data.result) {
    xAxis.push([i, key]);
    var key1 = data.result[key];
    for (var innerKey in key1) {
      label.push(innerKey)
      yAxis.push([i, key1[innerKey]])
    }
    i++;
  }
  this.render(xAxis, yAxis, unique(label));
}
StackedTile.prototype.render = function (xAxis, yAxis, label) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  $.plot(ctx, [yAxis], {
    series: {
      stack: true
      , bars: {
        show: true
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
      , borderColor: "#EEEEEE"
    }
    , bars: {
      align: "center"
      , horizontal: false
      , barWidth: .8
      , lineWidth: 0
    }
    , xaxis: {
      ticks: xAxis,
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
      /*function(label, x, y) {
                  var date = new Date(x);
                  return label + ": " + y + " at " + date;
                  }*/
        "%s: %y events at %x"
      , defaultFormat: true
    }
  });
}