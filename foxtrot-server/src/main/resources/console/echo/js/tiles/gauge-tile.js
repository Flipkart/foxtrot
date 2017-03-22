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
  var nestingArray = [];
  nestingArray.push(currentFieldList[parseInt(nesting)].field);
  return {
    "nesting": nestingArray,
  }
}

GaugeTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var data = {
    "opcode": "histogram",
    "table": object.table,
    "filters": object.filters,
    "field": "_timestamp",
    "period": "hours",
    "uniqueCountOn": object.uniqueCountOn && object.uniqueCountOn != "none" ? object.uniqueCountOn : null
  }
  $.ajax({
    method: "post",
    dataType: 'json',
    accepts: {
        json: 'application/json'
    },
    url: "http://foxtrot.traefik.prod.phonepe.com/foxtrot/v1/analytics",
    contentType: "application/json",
    data: JSON.stringify(data),
    success: $.proxy(this.getData, this)
  });
}

GaugeTile.prototype.getData = function(data) {
  if(data.counts == undefined || data.counts.length == 0)
    return;
  var chartData = [];
  for(var i = 0; i< data.counts.length; i++) {
    var date = new Date(data.counts[i].period);
    chartData.push({axis:formatDate(date), value: data.counts[i].count});
  }
  this.render(chartData);
}

GaugeTile.prototype.render = function (data) {
  var newDiv = this.newDiv;
  var object = this.object;
  var d = [data];
  var chartDiv = newDiv.find(".chart-item");
  chartDiv.addClass("gauge-chart");

  var minNumber = 1;
  var maxNumber = 100

  var randomNumber = Math.floor(Math.random()*(maxNumber-minNumber+1)+minNumber);

  var findExistingChart = chartDiv.find("#gauge-"+object.id);
  if(findExistingChart.length != 0) {
    findExistingChart.remove();
  }

  chartDiv.append('<div id="gauge-'+object.id+'"><div class="halfDonut"><div class="halfDonutChart"></div><div class="halfDonutTotal" data-percent="'+randomNumber+'" data-color="#f06961">'+randomNumber+'</div></div></div>')
  var ctx = chartDiv.find("#gauge-"+object.id);
  var donutDiv = ctx.find(".halfDonutChart");
  $(donutDiv).each(function (index, chart) {
    var value = $(chart).next(),
        percent = value.attr('data-percent'),
        color = value.attr('data-color');
    $.plot(chart, [{
        data: percent,
        color: "#9BA3AB"
    },{
        data: 100 - percent,
        color: "#E9E9E9"
    },{
        data: 100,
        color: '#ffffff'
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
