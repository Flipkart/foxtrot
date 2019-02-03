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
function LineRatioTile() {
    this.object = "";
  }
  
  function getLineRatioChartFormValues() {
    var period = $("#line-ratio-time-unit").val();
    var timeframe = $(".line-ratio-timeframe").val();
    var chartField = $("#line-ratio-field").val();
    var uniqueKey = $("#line-ratio-uniquekey").val();  
    //var ignoreDigits = $(".stackedBar-ignored-digits").val();
    var numeratorField = $("#line-ratio-numerator-field").val();
    var denominatorField = $("#line-ratio-denominator-field").val();
    
    if(uniqueKey == "none" || uniqueKey == "" || uniqueKey == null) {
      uniqueKey = null;
    } else {
      uniqueKey = currentFieldList[parseInt(uniqueKey)].field
    }
  
    if (chartField == "none") {
      return [[], false];
    }
    chartField = currentFieldList[parseInt(chartField)].field;
    return {
      "period": period
      , "timeframe": timeframe
      , "uniqueKey": uniqueKey
      , "lineRatioField": chartField
      , "numerator" : numeratorField
      , "denominator" : denominatorField
    };
  }
  
  function setLineRatioChartFormValues(object) {
    $("#line-ratio-time-unit").val(object.tileContext.period);
    $("#line-ratio-time-unit").selectpicker('refresh');
    $("#line-ratio-timeframe").val(object.tileContext.timeframe);
    $("#line-ratio-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.lineRatioField)));
    $("#line-ratio-field").selectpicker('refresh');
    $("#line-ratio-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
    $("#line-ratio-uniquekey").selectpicker('refresh');
    //$(".line-ratio-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));

    $("#line-ratio-numerator-field").val((object.tileContext.numerator == undefined ? '' : object.tileContext.numerator));
    
      $("#line-ratio-denominator-field").val((object.tileContext.denominator == undefined ? '' : object.tileContext.denominator));
  }
  
  function clearLineRatioChartForm() {
    $('.lineRatioForm')[0].reset();
    $(".lineRatioForm").find('.selectpicker').selectpicker('refresh');
  }
  
  LineRatioTile.prototype.getQuery = function (object) {
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
      "opcode": "trend"
      , "table": object.tileContext.table
      , "filters": filters
      , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
      , "field": object.tileContext.lineRatioField
      , period: periodFromWindow(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
    }
    var refObject = this.object;
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
      ,error: function(xhr, textStatus, error) {
        showFetchError(refObject);
      }
    });
  }
  
  LineRatioTile.prototype.getData = function (data) {
    
    if(data.length == 0)
      showFetchError(this.object);
    else
      hideFetchError(this.object);

    var numerator = data.trends[this.object.tileContext.numerator];
    var denominator = data.trends[this.object.tileContext.denominator];
    
    if(numerator != undefined && denominator != undefined) {
      var newData = [];
      newData.push(['date', 'count']);
      var numeratorValue = 0;
      var denominotorValue = 0;
      for(var loopIndex = 0; loopIndex < denominator.length; loopIndex++) {

        
        if(numerator[loopIndex] !== void 0) {
          /* void 0 === undefined * See concern about ``undefined'' below. */
          /* index doesn't point to an undefined item. */
          numeratorValue = numerator[loopIndex].count;
        }

        
        if(denominator[loopIndex] !== void 0) {
          denominotorValue = denominator[loopIndex].count;
        }

        var percentage = (denominotorValue/numeratorValue*100);
        var percentageValue = isNaN(percentage) ?  0 : percentage;

        if(loopIndex > 0) { // dont plot index 0
          newData.push([denominator[loopIndex].period, (percentageValue / Math.pow(10, 0))]);          
        }
      }
      this.render(newData);
    } else {
      this.render(newData);
    }
  }

  LineRatioTile.prototype.render = function (rows) {
    var object = this.object;
    var chartDiv = $("#"+object.id).find(".chart-item");
    var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"];
    var ctx = chartDiv.find("#" + object.id);
    ctx.width(ctx.width - 100);
    ctx.height(fullWidgetChartHeight());
    var plot = $.plot(ctx, [
      {
        data: rows
        , color: "#FF69B4",
          }
    , ], {
      series: {
        stack: true,
        lines: {
          show: true
          , lineWidth: 1.0
          , color: "red"
          , fill: false
          , fillColor: {
            colors: [{
              opacity: 0.7
            }, {
              opacity: 0.1
            }]
          }
        }
        , points: {
          show: true
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

    function showTooltip(x, y, xValue, yValue) {
      var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
      $('<div id="flot-custom-tooltip"> <div class="tooltip-custom-content"><p class="">'+yValue+'</p><p class="tooltip-custom-date-text">' + moment(xValue).format(a) + '</p></div></div>').css( {
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
          var x = item.datapoint[0].toFixed(2),
              y = item.datapoint[1].toFixed(1);
          showTooltip(item.pageX, item.pageY, Number(x), y);
        }
      } else {
        $("#flot-custom-tooltip").remove();
        clicksYet = false;
        previousPoint = null;
      }
    });
    
  }
  