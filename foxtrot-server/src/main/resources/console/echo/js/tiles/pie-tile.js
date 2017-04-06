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
function PieTile() {
  this.newDiv = "";
  this.object = "";
}

function getPieChartFormValues() {
  var period = $(".pie-time-unit").val();
  var timeframe = $(".pie-timeframe").val();
  var chartField = $(".eventtype-field").val();
  var uniqueKey = $(".pie-uniquekey").val();
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
    , "eventFiled": chartField
  , }, status]
}

function setPieChartFormValues(object) {
  $(".pie-time-unit").val(object.period);
  $(".pie-time-unit").selectpicker('refresh');
  $(".pie-timeframe").val(object.timeframe);
  $("pie-timeframe").selectpicker('refresh');
  var stackingField = currentFieldList.findIndex(x => x.field == object.eventFiled);
  $(".eventtype-field").val(stackingField);
  $(".eventtype-field").selectpicker('refresh');
  var stackingUniqueField = currentFieldList.findIndex(x => x.field == object.uniqueKey);
  $(".pie-bar-uniquekey").val(stackingUniqueField);
  $(".pie-bar-uniquekey").selectpicker('refresh');
}

function clearPieChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  var timeUnitEl = parentElement.find(".pie-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  var timeframe = parentElement.find(".pie-timeframe");
  timeframe.val('');
  var stackingKey = parentElement.find(".eventtype-field");
  stackingKey.find('option:eq(0)').prop('selected', true);
  $(stackingKey).selectpicker('refresh');
  var stackingBarUniqueKey = parentElement.find(".pie-uniquekey");
  stackingBarUniqueKey.find('option:eq(0)').prop('selected', true);
  $(stackingBarUniqueKey).selectpicker('refresh');
}
PieTile.prototype.getQuery = function (newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  this.object.filters.pop();
  object.filters.push(timeValue(object.period, object.timeframe, getPeriodSelect(object.id)))
  var data = {
    "opcode": "group"
    , "table": object.table
    , "filters": object.filters
    , "uniqueCountOn": object.uniqueCountOn && object.uniqueCountOn != "none" ? object.uniqueCountOn : null
    , nesting: [object.eventFiled]
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
PieTile.prototype.getData = function (data) {
  var colors = new Colors(Object.keys(data.result).length);
  var columns = [];
  this.uniqueValues = [];
  for (property in data.result) {
    /*if (this.isValueVisible(property)) {

    }*/
    columns.push({
      label: property
      , data: data.result[property]
      , color: colors.nextColor()
      , lines: {
        show: true
      }
      , shadowSize: 0
    });
    this.uniqueValues.push(property);
  }
  this.render(columns)
}
PieTile.prototype.render = function (columns) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  var chartOptions = {
    series: {
      pie: {
        innerRadius: 0.8
        , show: true
        , label: {
          show: false
        }
      }
    }
    , legend: {
      show: false
    }
    , grid: {
      hoverable: true
    }
    , tooltip: true
    , tooltipOpts: {
      content: function (label, x, y) {
        return label + ": " + y;
      }
    }
  };
  $.plot(ctx, columns, chartOptions);
  //drawLegend(columns, ctx.find(".legend"));
}
