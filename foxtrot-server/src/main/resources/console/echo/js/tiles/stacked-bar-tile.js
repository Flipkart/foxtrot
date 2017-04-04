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

  console.log('**')
  console.log(chartField)
  if(chartField == "none") {
    return[[], false];
  }

  chartField = currentFieldList[parseInt(chartField)].field;
/*
  var stackingString = currentFieldList[parseInt(stackingKey)].field;
  var nestingArray = [];

  nestingArray.push(groupingString);
  nestingArray.push(stackingString);
*/

  console.log('==')
  var status = true;

  if(!$("#stacked-bar-time-unit").valid() || !$("#stacked-bar-timeframe").valid()) {
    status = false;
  }
  console.log('--')
  return [{
    "period": period,
    "timeframe": timeframe,
    "uniqueKey": uniqueKey,
    "stackedBarField": chartField,
  }, status]
}

function setStackedBarChartFormValues(object) {
  $(".stacked-bar-time-unit").val(object.period);
  $("stacked-bar-time-unit").selectpicker('refresh');

  $(".stacked-bar-timeframe").val(object.periodValue);

  var stackingField = currentFieldList[parseInt(object.stackedBarField)].field;
  $(".stacked-bar-field").val(currentFieldList.findIndex(x => x.field == groupingKeyField));
  $(".stacked-bar-field").selectpicker('refresh');

  var stackingUniqueField = currentFieldList[parseInt(object.uniqueKey)].field;
  $(".stacked-bar-uniquekey").val(currentFieldList.findIndex(x => x.field == stackingUniqueField));
  $(".stacked-bar-uniquekey").selectpicker('refresh');
}

function clearStackedBarChartForm() {
  var parentElement = $("#"+currentChartType+"-chart-data");

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
}

StackedBarTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  this.object.filters.pop();
  var ts = new Date().getTime();
  var duration = object.timeframe+object.period;
  object.filters.push( {field: "_timestamp", operator: "last", duration: duration, currentTime: ts})
  var data = {
    "opcode": "trend",
    "table": object.table,
    "filters": object.filters,
    "uniqueCountOn": object.uniqueCountOn && object.uniqueCountOn != "none" ? object.uniqueCountOn : null,
    "field" : object.stackedBarField,
    period: periodFromWindow(object.period, "custom")
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

function unique(list) {
    var result = [];
    $.each(list, function(i, e) {
        if ($.inArray(e, result) == -1) result.push(e);
    });
    return result;
}

StackedBarTile.prototype.getData = function(data) {
  if(data.result == undefined || data.result.length == 0)
    return;
  var xAxis = [];
  var yAxis = [];
  var label = [];
  var i = 0;
  for (var key in data.result){
    xAxis.push([i, key]);
    var key1 = data.result[key];
    for(var innerKey in key1) {
      label.push(innerKey)
      yAxis.push([i,key1[innerKey]])
    }
    i++;
  }
  this.render(xAxis, yAxis,unique(label));
}

StackedBarTile.prototype.render = function (xAxis, yAxis, label) {
  var newDiv = this.newDiv;
  var object = this.object;
	var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#"+object.id);
	ctx.width(ctx.width);
	ctx.height(230);
	$.plot(ctx, [yAxis], {
        series: {
            stack: true,
          bars: {
                show: true
            }
        },
        grid: {
            hoverable: true,
            color: "#B2B2B2",
            show: true,
            borderWidth: 1,
            borderColor: "#EEEEEE"
        },
    bars: {
            align: "center",
            horizontal: false,
            barWidth: .8,
            lineWidth: 0
        },
        xaxis: {
          ticks: xAxis
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: /*function(label, x, y) {
             var date = new Date(x);
             return label + ": " + y + " at " + date;
             }*/"%s: %y events at %x",
            defaultFormat: true
        }
    });
}
