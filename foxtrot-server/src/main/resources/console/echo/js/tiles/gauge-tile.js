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

  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "period": period
    , "timeframe": timeframe
    , "successField" : successField
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
  var data = {
    "opcode": "group"
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
      showFetchError(refObject);
    }
  });
}
GaugeTile.prototype.getData = function (data) {

  if(data.length == 0)
    showFetchError(this.object);
  else
    hideFetchError(this.object);
    
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
    if(successField == key) {
      successRate = value;
    }
  }
  this.render(100, (successRate/total*100));
}
GaugeTile.prototype.render = function (total, diff) {
  var object = this.object;
  var d = [total];
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("gauge-chart");
  var minNumber = 1;
  var findExistingChart = chartDiv.find("#gauge-" + object.id);
  if (findExistingChart.length != 0) {
    findExistingChart.remove();
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
}
