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
// ------- Starts added today yesterday and daybefore yesterday---------------
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
      "opcode": "trend"
      ,"consoleId": getCurrentConsoleId()
      , "table": object.tileContext.table
      , "filters": filters
      , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
      , "field": object.tileContext.lineRatioField
      , period: periodFromWindow(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
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
  
  LineRatioTile.prototype.getData = function (data) {
    var dataLength = Object.keys(data.trends).length;
   if(dataLength > 0) {
      /**
   * Check special character exist
   * if exist split by space
   * loop array and get values from response
   * if unable to get value from response
   * set value as zero
   * eval numerator and denominator strings
   */
  var format = /^[ !@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/;
  var numeratorObject =  {};
  var numeratorCount = [];
  var denominatorCount = [];
  var denominatorObject = {};
  
  var numerator = this.object.tileContext.numerator;
  var denominator = this.object.tileContext.denominator;

  /**
   * Separate numerator and denominator and store count of that
   */
  if(isSpecialCharacter(numerator)) {
    var numeratorSplitArray = numerator.split(" ");
    for(var i = 0; i < numeratorSplitArray.length; i++) {
      if(!format.test(numeratorSplitArray[i])) { // check string or special character
        var string = data.trends[numeratorSplitArray[i]];
        numeratorObject[numeratorSplitArray[i]] =  data.trends[numeratorSplitArray[i]];
        numeratorCount.push(data.trends[numeratorSplitArray[i]].length);
      }
    }
  } else {
    numeratorObject[numerator] = data.trends[numerator];
    numeratorCount.push(data.trends[numerator].length);
  }

  if(isSpecialCharacter(denominator)) {
    var denominatorSplitArray = denominator.split(" ");
    for(var i = 0; i < denominatorSplitArray.length; i++) {
      if(!format.test(denominatorSplitArray[i])) { // check string or special character
        var string = data.trends[denominatorSplitArray[i]];
        denominatorObject[denominatorSplitArray[i]] =  data.trends[denominatorSplitArray[i]];
        denominatorCount.push(data.trends[denominatorSplitArray[i]].length);
      }
    }
  } else {
    denominatorObject[denominator] = data.trends[denominator];
    denominatorCount.push(data.trends[denominator].length);
  }

  // find least number of iteration
  var combinedNumberOfCount = numeratorCount.concat(denominatorCount);
  var numberofIteration = Math.min.apply(null,combinedNumberOfCount);  
  
  var newData = [];
  newData.push(['date', 'count']);

  var finalNumerator = []; // to calculate formula
  var finalDenominator = [];

  var numeratorkeys; // separate keys alone
  var denominatorkeys;
  
  numeratorkeys = Object.keys(numeratorObject);
  denominatorkeys = Object.keys(denominatorObject);

  var evalString = "";
  var period = 0;
  
  // loop till figured out least number
  for(var resultLoop = 0; resultLoop < numberofIteration; resultLoop++ ) 
  {
    if(isSpecialCharacter(numerator)) {
      var resultNumerator = numerator.split(" ");
      for(var i = 0; i < resultNumerator.length; i++) {
        if(format.test(resultNumerator[i])) { // check string or special character
          evalString+= resultNumerator[i];
        } else {
          var string = numeratorObject[resultNumerator[i]][resultLoop].count;
          evalString+= string == undefined ? 0 : string;
          period = (numeratorObject[resultNumerator[i]][resultLoop].period == undefined ? 0 : numeratorObject[resultNumerator[i]][resultLoop].period);
        }          
      }
    } else {
      var countVariable = numeratorObject[numerator][resultLoop].count;
      evalString+= countVariable == undefined ? 0 : countVariable;
      period = (numeratorObject[numerator][resultLoop].period == undefined ? 0 : numeratorObject[numerator][resultLoop].period);
    }
    finalNumerator.push({"period": period, "count": eval(evalString)});
    evalString = "";
    period = 0;
  }

  for(var resultLoop = 0; resultLoop < numberofIteration; resultLoop++ ) 
  {
    if(isSpecialCharacter(denominator)) {
      var resultNumerator = denominator.split(" ");
      for(var i = 0; i < resultNumerator.length; i++) {
        if(format.test(resultNumerator[i])) { // check string or special character
          evalString+= resultNumerator[i];
        } else {
          var string = denominatorObject[resultNumerator[i]][resultLoop].count;
          evalString+= string == undefined ? 0 : string;
          period = (denominatorObject[resultNumerator[i]][resultLoop].period == undefined ? 0 : denominatorObject[resultNumerator[i]][resultLoop].period);
        }          
      }
    } else {
      var countVariable = denominatorObject[denominator][resultLoop].count;
      evalString+= countVariable == undefined ? 0 : countVariable;
      period = (denominatorObject[denominator][resultLoop].period == undefined ? 0 : denominatorObject[denominator][resultLoop].period);
    }
    finalDenominator.push({"period": period, "count": eval(evalString)});
    evalString = "";
    period = 0;
  }

  // Loop final value and calculate percentage
  for(var finalValue = 0; finalValue < finalNumerator.length; finalValue++) {
    var denominatorTotal;
    var numeratorTotal;

    if(finalDenominator[finalValue]) { // check value exist
      denominatorTotal = finalDenominator[finalValue].count
    }

    if(finalNumerator[finalValue]) {// check value exist
      numeratorTotal = finalNumerator[finalValue].count;
    }

    var percentage = (denominatorTotal/numeratorTotal*100);

    if(isNaN(percentage))// if nan
      percentage = 0;

    if(percentage == "Infinity") // if 4/0*100 = infinity, if this is true set percentage as 0
      percentage = 0;

    newData.push([finalNumerator[finalValue].period, (percentage / Math.pow(10, 0))]); 
  }
  this.render(newData, dataLength);
  } else {
    this.render([], dataLength);
  }
}

  LineRatioTile.prototype.render = function (rows, dataLength) {
    if(dataLength == 0)
      showFetchError(this.object, "data", null);
    else
      hideFetchError(this.object);

    var object = this.object;
    var chartDiv = $("#"+object.id).find(".chart-item");
    var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"];
    var ctx = chartDiv.find("#" + object.id);

    if(dataLength == 0) {
      ctx.hide();
    } else {
      ctx.show();
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
  }


//  -------------------- Start added download widget 2--------------------


  LineRatioTile.prototype.downloadWidget = function (object) {
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
      "opcode": "trend"
      , "table": object.tileContext.table
      , "filters": filters
      , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
      , "field": object.tileContext.lineRatioField
      , period: periodFromWindow(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
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
        downloadTextAsCSV(response, 'LineRatio.csv')
      },
      error: function(xhr, textStatus, error ) {
        console.log("error.........",error,textStatus,xhr)
      }
  });
  }

//  -------------------- Ends added download widget 2--------------------
