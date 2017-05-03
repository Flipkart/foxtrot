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
  this.newDiv = "";
  this.object = "";
}

function getBarChartFormValues() {
  var period = $(".bar-time-unit").val();
  var timeframe = $(".bar-timeframe").val();
  var eventField = $(".bar-event-field").val();
  var uniqueKey = $(".bar-uniquekey").val();
  var ignoreDigits = $(".bar-ignored-digits").val();
  if (eventField == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(eventField)].field;
  var status = true;
  if (!$("#bar-time-unit").valid() || !$("#bar-timeframe").valid()) {
    status = false;
  }
  return [{
    "period": period
    , "timeframe": timeframe
    , "nesting": [groupingString]
    , "uniqueKey": uniqueKey
    , "ignoreDigits" : ignoreDigits
  }, status]
}

function setBarChartFormValues(object) {
  console.log(object)
  $(".bar-time-unit").val(object.tileContext.period);
  $(".bar-time-unit").selectpicker('refresh');
  $(".bar-timeframe").val(object.tileContext.timeframe);
  $(".bar-event-field").val(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0]));
  $(".bar-event-field").selectpicker('refresh');
  $(".bar-uniquekey").val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey));
  $(".bar-uniquekey").selectpicker('refresh');
  $(".bar-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
}

function clearBarChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  var timeUnitEl = parentElement.find(".bar-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  var timeframe = parentElement.find(".bar-timeframe");
  timeframe.val('');
  var groupingKey = parentElement.find(".bar-event-field");
  groupingKey.find('option:eq(0)').prop('selected', true);
  $(groupingKey).selectpicker('refresh');
  var stackingBarUniqueKey = parentElement.find(".bar-uniquekey");
  stackingBarUniqueKey.find('option:eq(0)').prop('selected', true);
  $(stackingBarUniqueKey).selectpicker('refresh');
}
BarTile.prototype.getQuery = function (newDiv, object) {
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
BarTile.prototype.getData = function (data) {
  this.object.tileContext.filters.pop();
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
  for (property in data.result) {
    var value = data.result[property] / Math.pow(10, this.object.tileContext.ignoreDigits);
    var visible = $.inArray( property, this.object.tileContext.uiFiltersSelectedList);
    if((visible == -1 ? true : false)) {
      var dataElement = {
        label: property
        , data: [[i, value]]
        , color: colors.nextColor()
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
  /*if (tmpLabel.visualLength() <= parentWidth) {
    xAxisOptions['ticks'] = ticks;
    xAxisOptions['tickFormatter'] = null;
  }
  else {
    xAxisOptions['ticks'] = null;
    xAxisOptions['tickFormatter'] = function () {
      return "";
    }
  }*/
  xAxisOptions['ticks'] = null;
  xAxisOptions['tickFormatter'] = function () {
    return "";
  }
  this.render(xAxisOptions, columns);
  console.log(xAxisOptions);
}
BarTile.prototype.render = function (xAxisOptions, columns) {
  console.log(columns);
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = newDiv.find(".chart-item");
  console.log(object.id);
  var ctx = chartDiv.find("#" + object.id);
  ctx.width(ctx.width);
  ctx.height(230);
  var chartOptions = {
    series: {
      bars: {
        show: true
        , label: {
          show: true
        }
        , barWidth: 0.5
        , align: "center"
        , lineWidth: 1.0
        , fill: true
        , fillColor: {
          colors: [{
            opacity: 0.3
          }, {
            opacity: 0.7
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
    , tooltip: true
    , tooltipOpts: {
      content: function (label, x, y) {
        return label + ": " + y;
      }
    }
    ,legend: {
      show: true
      , noColumns: getLegendColumn(object.tileContext.widgetType)
      , labelFormatter: function (label, series) {
        return '<span class="legend-custom"> &nbsp;' + label + ' &nbsp;</span>';
      }
      , container: $(chartDiv.find(".legend"))
    }
  };
  $.plot(ctx, columns, chartOptions);
}
