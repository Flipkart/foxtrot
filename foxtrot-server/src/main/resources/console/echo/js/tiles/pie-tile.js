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
  this.object = "";
}

function getPieChartFormValues() {
  var period = $("#pie-time-unit").val();
  var timeframe = $("#pie-timeframe").val();
  var chartField = $("#eventtype-field").val();
  var uniqueKey = $("#pie-uniquekey").val();
  var ignoreDigits = $(".pie-ignored-digits").val();
  var selectedValue = $("#pie-selected-value").val();
  var aggregationType = $('#pie-aggregation-type').val();
  var aggregationField = $('#pie-aggregation-field').val();
  

  if (chartField == "none") {
    return [[], false];
  }

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

  chartField = currentFieldList[parseInt(chartField)].field;
  return {
    "period": period
    , "timeframe": timeframe
    , "uniqueKey": uniqueKey
    , "eventFiled": chartField
    , "ignoreDigits" : ignoreDigits
    , "selectedValue": selectedValue
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
    , };
}

function setPieChartFormValues(object) {
  $("#pie-time-unit").val(object.tileContext.period);
  $("#pie-time-unit").selectpicker('refresh');
  $("#pie-timeframe").val(object.tileContext.timeframe);
  $("#pie-timeframe").selectpicker('refresh');
  var stackingField = currentFieldList.findIndex(x => x.field == object.tileContext.eventFiled);
  $("#eventtype-field").val(parseInt(stackingField));
  $("#eventtype-field").selectpicker('refresh');
  var stackingUniqueField = currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey);
  $("#pie-uniquekey").val(parseInt(stackingUniqueField));
  $("#pie-uniquekey").selectpicker('refresh');
  $(".pie-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  $("#pie-selected-value").val((object.tileContext.selectedValue == undefined ? '' : object.tileContext.selectedValue));
  $('#pie-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#pie-aggregation-field').selectpicker('refresh');
  $('#pie-aggregation-Type').val();
}

function clearPieChartForm() {
  $('.pieForm')[0].reset();
  $(".pieForm").find('.selectpicker').selectpicker('refresh');
}

PieTile.prototype.getQuery = function (object) {
  this.object = object;
  var filters = [];
  // ------- Starts added  today yesterday and daybefore yesterday---------------
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
    "consoleId": getCurrentConsoleId()
  }
  
  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , nesting: [object.tileContext.eventFiled]
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
PieTile.prototype.getData = function (data) {
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
        , color: convertHex(colors.nextColor(), 100)
        , lines: {
          show: true
        }
        , shadowSize: 0
      });
    }
    this.object.tileContext.uiFiltersList.push(property);
  }
  this.render(columns, Object.keys(data.result).length)
}
PieTile.prototype.render = function (columns, dataLength) {

  if(dataLength == 0) {
    showFetchError(this.object, "data", null);
  } else {
    hideFetchError(this.object);
  }

  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);
  ctx.addClass('col-sm-7');
  ctx.css('margin-top', '35px');
  $("#"+object.id).find(".chart-item").find(".legend").addClass('pie-legend');
  $("#"+object.id).find(".chart-item").css('margin-top', "53px");

  if(dataLength == 0) {
    ctx.hide();
    $(chartDiv).find(".legend").hide();
  } else {
    $(chartDiv).find(".legend").show();
    ctx.show();
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
        show: false
      }
      , grid: {
        hoverable: true
      }
      , tooltip: true
      , tooltipOpts: {
        content: function (label, x, y, point) {
          $("#"+object.id).find(".chart-item").find(".pie-center-div").show();
          $("#"+object.id).find(".chart-item").find(".pie-center-div").find('.pie-center-value').text(y+"("+point.series.percent.toFixed(1) + '%)');
          $("#"+object.id).find(".chart-item").find(".pie-center-div").find('.pie-center-label').text(label);
          return "" + ": " + "";
        }
        ,onHover: function(flotItem, $tooltipEl) {
          $tooltipEl.hide();
        }
      }
    };
  
  
    var plot = $.plot(ctx, columns, chartOptions);
  
    $("#"+object.id).find(".chart-item").find("#"+object.id).append('<div class="pie-center-div"><div><p class="pie-center-value"></p><hr/><p class="pie-center-label"></p></div></div>');
  
    drawPieLegend(columns, $(chartDiv.find(".legend")));
    var re = re = /\(([0-9]+,[0-9]+,[0-9]+)/;
    $(chartDiv.find('.legend ul li')).on('mouseenter', function() {
      var label = $(this).text();
      label = label.substring(0, label.indexOf('-'));
      var allSeries = plot.getData();
      for (var i = 0; i < allSeries.length; i++){
        if (allSeries[i].label == $.trim(label)){
          allSeries[i].oldColor = allSeries[i].color;
          allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 1 + ')';
          $("#"+object.id).find(".chart-item").find(".pie-center-div").show();
          $("#"+object.id).find(".chart-item").find(".pie-center-div").find('.pie-center-value').text(allSeries[i].data[0][1]+"("+allSeries[i].percent.toFixed(1) + '%)');
          $("#"+object.id).find(".chart-item").find(".pie-center-div").find('.pie-center-label').text(allSeries[i].label);
        } else {
          allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 0.1 + ')';
        }
      }
      plot  .draw();
    });
  
    $(chartDiv.find('.legend ul li')).on('mouseleave', function() {
      var label = $(this).text();
      label = label.substring(0, label.indexOf('-'));
      var allSeries = plot.getData();
      for (var i = 0; i < allSeries.length; i++){
        allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 1 + ')';
        if (allSeries[i].label == $.trim(label)){
          allSeries[i].color = allSeries[i].oldColor;
          $("#"+object.id).find(".chart-item").find(".pie-center-div").hide();
        }
      }
      plot.draw();
    });
    $(ctx).bind("plothover", function (event, pos, item) {
      if(!item) {
        $("#"+object.id).find(".chart-item").find(".pie-center-div").hide();
      }
    });
  }
}



//  -------------------- Starts added download widget 2--------------------

PieTile.prototype.downloadWidget = function (object) {
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
    "consoleId": getCurrentConsoleId()
  }

  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , nesting: [object.tileContext.eventFiled]
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
      downloadTextAsCSV(response, 'PieChart.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}

//  -------------------- Ends added download widget 2--------------------
