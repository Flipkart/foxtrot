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
  var status = false;

  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray
    , "period": period
    , "timeframe": timeframe
    , "numerator" : numeratorField
    , "denominator" : denominatorField
  };
}

function setPercentageGaugeChartFormValues(object) {
  var selectedNesting = object.tileContext.nesting.toString();
  var selectedNestingArrayIndex = parseInt(currentFieldList.findIndex(x => x.field == selectedNesting));
  var nesting = $("#percentage-gauge-nesting").val(selectedNestingArrayIndex);
  $("#percentage-gauge-nesting").selectpicker('refresh');

  var timeUnit = $("#percentage-gauge-time-unit").val(object.tileContext.period);
  timeUnit.selectpicker('refresh');

  $("#percentage-gauge-timeframe").val(object.tileContext.timeframe)

  $("#percentage-gauge-numerator-field").val((object.tileContext.numerator == undefined ? '' : object.tileContext.numerator));

  $("#percentage-gauge-denominator-field").val((object.tileContext.denominator == undefined ? '' : object.tileContext.denominator));
}

function clearPercentageGaugeChartForm() {
  $('.percentageGaugeForm')[0].reset();
  $(".percentageGaugeForm").find('.selectpicker').selectpicker('refresh');
}
PercentageGaugeTile.prototype.getQuery = function (object) {
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
  for (var key in data.result) {
    var value = data.result[key];
    total = total + value;
    if(numerator == key) {
      numerator = value;
    } else if(denominator == key) {
      denominator = value;
    }
  }
  this.render(total, (denominator), (denominator/numerator*100));
}
PercentageGaugeTile.prototype.render = function (total, successRate, diff) {
  var object = this.object;
  var d = [total];
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("percentage-gauge-chart");
  var minNumber = 1;
  var maxNumber = total
  var findExistingChart = chartDiv.find("#percentage-gauge-" + object.id);
  if (findExistingChart.length != 0) {
    findExistingChart.remove();
  }

  chartDiv.append('<div id="percentage-gauge-' + object.id + '"><div class="halfDonut"><div class="halfDonutChart"></div><div class="halfDonutTotal bold gauge-percentage" data-percent="' + successRate + '" data-color="#82c91e">' + Math.round(diff) + '%</div></div></div>')
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
}
