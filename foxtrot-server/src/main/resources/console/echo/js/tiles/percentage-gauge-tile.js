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
function PercentageGaugeTile() {
  this.object = "";
}

function getPercentageGaugeChartFormValues() {
  var nesting = $("#percentage-gauge-nesting").val();
  var timeframe = $("#percentage-gauge-timeframe").val();
  var period = $("#percentage-gauge-time-unit").val();
  var numeratorField = $("#percentage-gauge-numerator-field").val();
  var denominatorField = $("#percentage-gauge-denominator-field").val();
  var thresholdField = $("#percentage-gauge-threshold-field").val();
  var status = false;
  var uniqueKey = $("#percentage-gauge-uniquekey").val();
  var aggregationType = $('#percentage-gauge-aggregation-type').val();
  var aggregationField = $('#percentage-gauge-aggregation-field').val();
  var markDecimal = $('#mark-decimal').is(":checked");

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

  
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "period": period
    , "timeframe": timeframe
    , "numerator" : numeratorField
    , "denominator" : denominatorField
    , "uniqueKey": uniqueKey
    , "threshold": thresholdField
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
    , "markDecimal":markDecimal
  }; 
}

function setPercentageGaugeChartFormValues(object) {
  var selectedNesting = object.tileContext.nesting.toString();
  var selectedNestingArrayIndex = parseInt(currentFieldList.findIndex(x => x.field == selectedNesting));
  var nesting = $("#percentage-gauge-nesting").val(selectedNestingArrayIndex);
  $("#percentage-gauge-nesting").selectpicker('refresh');

  var timeUnit = $("#percentage-gauge-time-unit").val(object.tileContext.period);
  timeUnit.selectpicker('refresh');

  $("#percentage-gauge-timeframe").val(object.tileContext.timeframe);
  $('#mark-decimal').prop("checked",object.tileContext.markDecimal).change();

  $("#percentage-gauge-numerator-field").val((object.tileContext.numerator == undefined ? '' : object.tileContext.numerator));

  $("#percentage-gauge-denominator-field").val((object.tileContext.denominator == undefined ? '' : object.tileContext.denominator));

  var stackingUniqueField = currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey);
  $("#percentage-gauge-uniquekey").val(parseInt(stackingUniqueField));
  $("#percentage-gauge-uniquekey").selectpicker('refresh');

  var threshold = object.tileContext.threshold == undefined ? '' : object.tileContext.threshold;
  $("#percentage-gauge-threshold-field").val(threshold);

  $('#percentage-gauge-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#percentage-gauge-aggregation-field').selectpicker('refresh');
  $('#percentage-gauge-aggregation-Type').val();
  $("#mark-extrapolation-percentageGauge-chart").prop("checked", object.tileContext.markDecimal).change();
}

function clearPercentageGaugeChartForm() {
  $('.percentageGaugeForm')[0].reset();
  $(".percentageGaugeForm").find('.selectpicker').selectpicker('refresh');
}
PercentageGaugeTile.prototype.getQuery = function (object) {
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
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "nesting": object.tileContext.nesting
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    ,"aggregationField":object.tileContext.aggregationField
    ,"aggregationType":object.tileContext.aggregationType
    ,"sourceType":"ECHO_DASHBOARD"
    ,"requestTags": requestTags
    ,"extrapolationFlag": false
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
PercentageGaugeTile.prototype.getData = function (data) {    
  var numerator = 0;
  var denominator = 0;

  if(this.object.tileContext.numerator) {
    numerator = this.object.tileContext.numerator;
  }

  if(this.object.tileContext.numerator) {
    denominator = this.object.tileContext.denominator;
  }

  if (data.result == undefined || data.result.length == 0) return;
  var total = 0;

  /**
   * Check special character exist
   * if exist split by space
   * loop array and get values from response
   * if unable to get value from response
   * set value as zero
   * eval numerator and denominator strings
   */
  var format = /^[ !@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/;
  var numeratorStringEval = "";  
  var denominatorStringEval = "";
  
  if(isSpecialCharacter(numerator)) {
    var numeratorSplitArray = numerator.split(" ");
    for(var i = 0; i < numeratorSplitArray.length; i++) {
      if(format.test(numeratorSplitArray[i])) { // check string or special character
        numeratorStringEval+= numeratorSplitArray[i];
      } else {
        var string = data.result[numeratorSplitArray[i]];
        numeratorStringEval+= string == undefined ? 0 : string;
      }
    }
  } else {
    numeratorStringEval = data.result[numerator];
  }

  if(isSpecialCharacter(denominator)) {
    var denominatorSplitArray = denominator.split(" ");
    for(var i = 0; i < denominatorSplitArray.length; i++) {
      if(format.test(denominatorSplitArray[i])) { // check string or special character
        denominatorStringEval+= denominatorSplitArray[i];
      } else {
        var string = data.result[denominatorSplitArray[i]];
        denominatorStringEval+= string == undefined ? 0 : string;
      }
    }
  } else {
    denominatorStringEval = data.result[denominator];
  }

  this.render(100, (eval(denominatorStringEval)/eval(numeratorStringEval)*100), Object.keys(data.result).length);
}
PercentageGaugeTile.prototype.render = function (total, diff, dataLength) {

  if(dataLength == 0) {
    showFetchError(this.object, "data", null);
  } else {
    hideFetchError(this.object);
  }

  var object = this.object;
  var d = [total];
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("percentage-gauge-chart");


  var minNumber = 1;
  var findExistingChart = chartDiv.find("#percentage-gauge-" + object.id);
  if (findExistingChart.length != 0) {
    findExistingChart.remove();
  }

  // if percentage is less than threshold configured in widget
  var thresholdError = chartDiv.find(".threshold-msg");    
  if(this.object.tileContext.threshold) {
    if(diff < this.object.tileContext.threshold)
    {
      if($(thresholdError).length == 0 ) {
        $(chartDiv).append("<p class='threshold-msg'>"+thresholdErrorMsg()+"</p>");
      } else {
        $(chartDiv).find(".threshold-msg").show();
      }
      $(chartDiv).find(".threshold-msg").show();
      return;
    } else {
      $(chartDiv).find(".threshold-msg").hide();
    }
  }

  const roundOff = $('#mark-decimal').is(":checked") || this.object.tileContext.markDecimal ? diff.toFixed(2) : Math.round(diff);

  chartDiv.append('<div id="percentage-gauge-' + object.id + '"><div class="halfDonut"><div class="halfDonutChart"></div><div class="halfDonutTotal bold gauge-percentage" data-percent="' + diff + '" data-color="#82c91e">' + roundOff + '%</div></div></div>')
  var ctx = chartDiv.find("#percentage-gauge-" + object.id);
  var donutDiv = ctx.find(".halfDonutChart");
  $(donutDiv).each(function (index, chart) {
    var value = $(chart).next()
    , percent = value.attr('data-percent')
    , color = value.attr('data-color');
    $.plot(chart, [{
      data: percent
      , color: "#82c91e"
    }, {
      data:total
      , color: "#eaeaea"
    }], {
      series: {
        pie: {
          show: true
          , innerRadius: .7
          , startAngle: 1
          , label: {
            show: false
          }
        }
      }
      , legend: {
        show: false
      }
    });
  });
  
  if(dataLength == 0) {
    ctx.hide();
  } else {
    ctx.show();
  }

}


PercentageGaugeTile.prototype.downloadWidget = function (object) {
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
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "nesting": object.tileContext.nesting
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
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
      downloadTextAsCSV(response, 'PercentageGauageChart.csv')
    },
    error: function(xhr, textStatus, error ) {
      console.log("error.........",error,textStatus,xhr)
    }
});
}