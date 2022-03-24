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
function LineTile() {
  this.object = "";
}

function getLineChartFormValues() {
  var period = $("#tile-time-unit").val();
  var uniqueCount = $("#uniqueKey").val();
  var timeframe = $("#line-timeframe").val();
  var ignoreDigits = $(".line-ignored-digits").val();

  if(uniqueCount == "none" || uniqueCount == "" || uniqueCount == null) {
    uniqueCount = null;
  } else {
    uniqueCount = currentFieldList[parseInt(uniqueCount)].field
  }

  return {
    "period": period
    , "uniqueCountOn": uniqueCount
    , "timeframe": timeframe
    , "ignoreDigits" : ignoreDigits
    , };
}

function setLineChartFormValues(object) {
  var parentElement = $("#" + object.tileContext.chartType + "-chart-data");
  var timeUnitEl = parentElement.find("#tile-time-unit");
  timeUnitEl.val(object.tileContext.period);
  $(timeUnitEl).selectpicker('refresh');
  var uniqeKey = parentElement.find("#uniqueKey");
  uniqeKey.val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueCountOn));
  $(uniqeKey).selectpicker('refresh');
  parentElement.find("#line-timeframe").val(object.tileContext.timeframe);
  $(".line-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
}

function clearLineChartForm() {
  $('.lineForm')[0].reset();
  $(".lineForm").find('.selectpicker').selectpicker('refresh');
}
LineTile.prototype.getQuery = function (object) {
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
// ------ Ends added today yesterday and daybefore yesterday--------------------

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
    "opcode": "histogram"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "field": "_timestamp"
    , "period": object.tileContext.period
    , "uniqueCountOn": object.tileContext.uniqueCountOn && object.tileContext.uniqueCountOn != "none" ? object.tileContext.uniqueCountOn : null
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
LineTile.prototype.getData = function (data) {
  var rows = [];
  if (data.counts && data.counts.length > 0) {
    rows.push(['date', 'count']);
    for (var i = data.counts.length - 1; i >= 0; i--) {
      rows.push([data.counts[i].period, (data.counts[i].count / Math.pow(10, this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits))]);
    }
  }
  this.render(rows);
}
LineTile.prototype.render = function (rows) {

  if(rows.length == 0)
    showFetchError(this.object, "data", null);
  else
    hideFetchError(this.object);
    
  var object = this.object;
  var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"]
  var chartDiv = $("#"+object.id).find(".chart-item");
  var ctx = chartDiv.find("#" + object.id);

  if(rows.length == 0) {
    $(ctx).hide();
  } else {
    $(ctx).show();
    ctx.width(ctx.width - 100);
    ctx.height(fullWidgetChartHeight());
    var plot = $.plot(ctx, [
      {
        data: rows
        , color: "#75c400",
          }
    , ], {
      series: {
        lines: {
          show: true
          , lineWidth: 1.0
          , color: "#9bc95b"
          , fill: true
          , fillColor: {
            colors: [{
              opacity: 0.7
            }, {
              opacity: 0.1
            }]
          }
        }
        , points: {
          show: false
        }
        , shadowSize: 0
        , curvedLines: { active: true }
      }
      , xaxis: {
        tickLength: 0
        , mode: "time"
        , timezone: "browser"
        , timeformat: axisTimeFormat(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
      , }
      , yaxis: {
        markingsStyle: 'dashed',
          tickFormatter: function(val, axis) {
          return numDifferentiation(val);
        },
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
      , }
      , tooltip: false
      , tooltipOpts: {
        content: ""
      }
      , colors: [borderColorArray[Math.floor(Math.random()*borderColorArray.length)]]
    , });
  
  
    
  
    var healthParentDiv = $("#"+object.id).find(".widget-header")
    var healthDiv = healthParentDiv.find("#" + object.id + "-health");
    healthDiv.width(100);
    healthDiv.addClass('health-div');
    /*$.plot(healthDiv, [
      {
        data: rows
          }
    , ], {
      series: {
        lines: {
          show: true
        }
        , points: {
          show: false
        }
        , shadowSize: 0
        , curvedLines: { active: true }
      }
      , xaxis: {
        mode: "time"
        , timezone: "browser"
        , timeformat: axisTimeFormat(object.tileContext.period, getPeriodSelect(object.id))
      , }
      , grid: {
        color: "#B2B2B2"
        , show: false
        , borderWidth: {
          top: 0
          , right: 0
          , bottom: 1
          , left: 1
        }
        , borderColor: "#EEEEEE"
        , hoverable:false
      , }
      ,selection: {
        mode: "x",
        minSize: 1
      }
      , tooltip: false
      , tooltipOpts: {
        content: "%y events at %x"
        , defaultFormat: true
      }
      , colors: ['#000']
    , });*/
  
    function showTooltip(x, y, xValue, yValue) {
      var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
      $('<div id="flot-custom-tooltip"> <div class="tooltip-custom-content"><p class="">'+numDifferentiation(yValue)+'</p><p class="tooltip-custom-date-text">' + moment(xValue).format(a) + '</p></div></div>').css( {
        position: 'absolute',
        display: 'none',
        top: y - 60,
        left: x - 2,
      }).appendTo("body").fadeIn(200);
    }
  
    var previousPoint = null;
    $(ctx).bind("plothover", function (event, pos, item) {
      if (item) {
        if (previousPoint != item.datapoint) {
          previousPoint = item.datapoint;
  
          $("#flot-custom-tooltip").remove();
          var x = item.datapoint[0].toFixed(0),
              y = item.datapoint[1].toFixed(2);
          showTooltip(item.pageX, item.pageY, Number(x), y);
        }
      } else {
        $("#flot-custom-tooltip").remove();
        clicksYet = false;
        previousPoint = null;
      }
    });
  }
}


// ------------------------------line graph download widget-------------------------------------

LineTile.prototype.downloadWidget = function (object) {
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
    "opcode": "histogram"
    , "table": object.tileContext.table
    , "filters": filters
    , "field": "_timestamp"
    , "period": object.tileContext.period
    , "uniqueCountOn": object.tileContext.uniqueCountOn && object.tileContext.uniqueCountOn != "none" ? object.tileContext.uniqueCountOn : null
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
      downloadTextAsCSV(response, 'LineChart.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}