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
function GaugeTile() {
  this.object = "";
}

function getGaugeChartFormValues() {
  var nesting = $("#gauge-nesting").val();
  var timeframe = $("#gauge-timeframe").val();
  var period = $("#gauge-time-unit").val();
  var successField = $("#gauge-success-field").val();
  var status = false;
  var thresholdField = $("#gauge-threshold-field").val();

  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "period": period
    , "timeframe": timeframe
    , "successField" : successField
    , "threshold": thresholdField
  };
}

function setGaugeChartFormValues(object) {
  var selectedNesting = object.tileContext.nesting.toString();
  var selectedNestingArrayIndex = parseInt(currentFieldList.findIndex(x => x.field == selectedNesting));
  var nesting = $("#gauge-nesting").val(selectedNestingArrayIndex);
  $("#gauge-nesting").selectpicker('refresh');

  var timeUnit = $("#gauge-time-unit").val(object.tileContext.period);
  timeUnit.selectpicker('refresh');

  $("#gauge-timeframe").val(object.tileContext.timeframe)

  $("#gauge-success-field").val((object.tileContext.successField == undefined ? '' : object.tileContext.successField));

  var threshold = object.tileContext.threshold == undefined ? '' : object.tileContext.threshold;
  $("#gauge-threshold-field").val(threshold);
}

function clearGaugeChartForm() {
  var parentElement = $("#" + currentChartType + "-chart-data");
  var nestingEl = parentElement.find("#gauge-nesting");
  nestingEl.find('option:eq(0)').prop('selected', true);
  $(nestingEl).selectpicker('refresh');

  $("#gauge-success-field").val('');

  var timeUnitEl = parentElement.find("#gauge-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');

  parentElement.find("#gauge-timeframe").val('');
  parentElement.find("#gauge-threshold-field").val('');
}
GaugeTile.prototype.getQuery = function (object) {
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

  var templateFilters = isAppendTemplateFilters(object.tileContext.table);
  if(templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }

  var data = {
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "nesting": object.tileContext.nesting
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
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
    }
  });
}
GaugeTile.prototype.getData = function (data) {
    
  var successField = "";
  var successRate = 0;

  if(this.object.tileContext.successField) {
    successField = this.object.tileContext.successField;
  }

  if (data.result == undefined || data.result.length == 0) return;
  var total = 0;
  for (var key in data.result) {
    var value = data.result[key];
    total = total + value;
    // if(successField == key) {
    //   successRate = value;
    // }
  }



  /**
   * Check special character exist
   * if exist split by space
   * loop array and get values from response
   * if unable to get value from response
   * set value as zero
   * eval successField string
   */
  var format = /^[ !@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/;
  var successFieldStringEval = "";  
  
  if(isSpecialCharacter(successField)) {
    var successFieldSplitArray = successField.split(" ");
    for(var i = 0; i < successFieldSplitArray.length; i++) {
      if(format.test(successFieldSplitArray[i])) { // check string or special character
        successFieldStringEval+= successFieldSplitArray[i];
      } else {
        var string = data.result[successFieldSplitArray[i]];
        successFieldStringEval+= string == undefined ? 0 : string;
      }
    }
  } else {
    successFieldStringEval = data.result[successField];
  }
  
  this.render(100, (eval(successFieldStringEval)/total*100), Object.keys(data.result).length);
}
GaugeTile.prototype.render = function (total, diff, dataLength) {


  if(dataLength == 0) {
    showFetchError(this.object, "data", null);
  } else {
    hideFetchError(this.object);
  }

  var object = this.object;
  var d = [total];
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("gauge-chart");


  var minNumber = 1;
  var findExistingChart = chartDiv.find("#gauge-" + object.id);
  if (findExistingChart.length != 0) {
    findExistingChart.remove();
  }

  // if percentage is less than threshold configured in widget
  var thresholdError = chartDiv.find(".threshold-msg");
  if(this.object.tileContext.threshold) {
    if(diff < this.object.tileContext.threshold)
    {
      if($(thresholdError).length == 0 ) {
        $(chartDiv).append("<p class='threshold-msg gauge-threshold-msg'>"+thresholdErrorMsg()+"</p>");
      } else {
        $(chartDiv).find(".threshold-msg").show();
      }
      $(chartDiv).find(".threshold-msg").show();
      return;
    } else {
      $(chartDiv).find(".threshold-msg").hide();
    }
  }

  chartDiv.append('<div id="gauge-' + object.id + '"><div class="halfDonut"><div class="halfDonutChart"></div><div class="halfDonutTotal bold gauge-percentage" data-percent="' + diff + '" data-color="#82c91e">' + Math.round(diff) + '%</div></div></div>')
  var ctx = chartDiv.find("#gauge-" + object.id);
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
