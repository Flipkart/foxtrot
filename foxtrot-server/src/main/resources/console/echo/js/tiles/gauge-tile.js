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
  this.newDiv = "";
  this.object = "";
}

function getGaugeChartFormValues() {
  var nesting = $(".gauge-nesting").val();

  var status = false;
  if($("#gauge-nesting").valid()) {
    status = true;
  }
  if(nesting == "none") {
    return[[], false];
  }
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return [{
    "nesting": nestingArray,
  }, status]
}

function setGaugeChartFormValues(object) {
  var selectedNesting = object.nesting.toString();
  var selectedNestingArrayIndex = currentFieldList.findIndex(x => x.field== selectedNesting);
  var nesting = $(".gauge-nesting").val(selectedNestingArrayIndex);
  $(".gauge-nesting").selectpicker('refresh');
}

function clearGaugeChartForm() {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var nestingEl = parentElement.find(".gauge-nesting");
  nestingEl.find('option:eq(0)').prop('selected', true);
  $(nestingEl).selectpicker('refresh');

  var timeUnitEl = parentElement.find(".gauge-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
}

GaugeTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var ts = new Date().getTime();
  object.filters.push( {field: "_timestamp", operator: "last", duration: "24hours", currentTime: ts})
  var data = {
    "opcode": "group",
    "table": object.table,
    "filters": object.filters,
    "nesting": object.nesting
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

GaugeTile.prototype.getData = function(data) {
  if(data.result == undefined || data.result.length == 0)
    return;
  var percentage = 0;
  for (var key in data.result){
    var value = data.result[key];
    percentage = percentage+value;
  }
  this.render(percentage%100);
}

GaugeTile.prototype.render = function (data) {
  var newDiv = this.newDiv;
  var object = this.object;
  var d = [data];
  var chartDiv = newDiv.find(".chart-item");
  chartDiv.addClass("gauge-chart");

  var minNumber = 1;
  var maxNumber = 100

  var randomNumber = data;

  var findExistingChart = chartDiv.find("#gauge-"+object.id);
  if(findExistingChart.length != 0) {
    findExistingChart.remove();
  }

  chartDiv.append('<div id="gauge-'+object.id+'"><div class="halfDonut"><div class="halfDonutChart"></div><div class="halfDonutTotal bold gauge-percentage" data-percent="'+randomNumber+'" data-color="#82c91e">'+randomNumber+'%</div></div></div>')
  var ctx = chartDiv.find("#gauge-"+object.id);
  var donutDiv = ctx.find(".halfDonutChart");
  $(donutDiv).each(function (index, chart) {
    var value = $(chart).next(),
        percent = value.attr('data-percent'),
        color = value.attr('data-color');
    $.plot(chart, [{
        data: percent,
        color: "#82c91e"
    },{
        data: 100 - percent,
        color: "#eaeaea"
    },{
        data: 100,
        color: '#eaeaea'
    }], {
        series: {
            pie: {
                show: true,
                innerRadius: .7,
                startAngle: 1,
                label: {
                    show: false
                }
            }
        },
        legend: {
            show: false
        }
    });
  });
}
