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
  var ignoreDigits = $(".pie-ignored-digits").val();
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
    , "ignoreDigits" : ignoreDigits
  , }, status]
}

function setPieChartFormValues(object) {
  $(".pie-time-unit").val(object.tileContext.period);
  $(".pie-time-unit").selectpicker('refresh');
  $(".pie-timeframe").val(object.tileContext.timeframe);
  $("pie-timeframe").selectpicker('refresh');
  var stackingField = currentFieldList.findIndex(x => x.field == object.tileContext.eventFiled);
  $(".eventtype-field").val(stackingField);
  $(".eventtype-field").selectpicker('refresh');
  var stackingUniqueField = currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey);
  $(".pie-bar-uniquekey").val(stackingUniqueField);
  $(".pie-bar-uniquekey").selectpicker('refresh');
  $(".pie-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
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
  $(".pie-ignored-digits").val(0);
}
PieTile.prototype.getQuery = function (newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
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
    , nesting: [object.tileContext.eventFiled]
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
PieTile.prototype.getData = function (data) {
  this.object.tileContext.filters.pop();
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }
  var colors = new Colors(Object.keys(data.result).length);
  var columns = [];
  this.object.tileContext.uiFiltersList = [];
  for (property in data.result) {
    var visible = $.inArray( property, this.object.tileContext.uiFiltersSelectedList);
    if ((visible == -1 ? true : false)) {
      columns.push({
        label: property
        , data: data.result[property] / Math.pow(10, (this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits))
        , color: colors.nextColor()
        , lines: {
          show: true
        }
        , shadowSize: 0
      });
    }
    this.object.tileContext.uiFiltersList.push(property);
  }
  this.render(columns)
}
PieTile.prototype.render = function (columns) {
  var newDiv = this.newDiv;
  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.addClass('col-sm-7');
  $("#"+object.id).find(".chart-item").find(".legend").addClass('pie-legend');
  $("#"+object.id).find(".chart-item").css('margin-top', "53px");
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
    ,highlightSeries: {
      color: "#FF00FF"
    }
    , legend: {
      show: false,
      noColumns:getLegendColumn(object.tileContext.widgetType),
      container: $(chartDiv.find(".legend"))
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


  var plot = $.plot(ctx, columns, chartOptions);
  drawLegend(columns, $(chartDiv.find(".legend")));
  $(chartDiv.find('.legend ul li')).on('mouseenter', function() {
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

  $(chartDiv.find('.legend ul li')).on('mouseleave', function() {
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
