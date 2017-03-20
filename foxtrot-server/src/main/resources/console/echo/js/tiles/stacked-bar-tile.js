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

function StackedBarTile() {
  this.newDiv = "";
  this.object = "";
}

StackedBarTile.prototype.getQuery = function(newDiv, object) {
  this.newDiv = newDiv;
  this.object = object;
  var data = {
    "opcode": "histogram",
    "table": object.table,
    "filters": object.filters,
    "field": "_timestamp",
    "period": object.period,
    "uniqueCountOn": object.uniqueCountOn && object.uniqueCountOn != "none" ? object.uniqueCountOn : null
  }
  console.log(data);
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

StackedBarTile.prototype.getData = function(data) {
  if(data.counts == undefined || data.counts.length == 0)
    return;
  var xAxis = [];
  var yAxis = [];
  for(var i = 0; i< data.counts.length; i++) {
    var date = new Date(data.counts[i].period);
    xAxis.push([i, formatDate(date)]);
    yAxis.push([i, data.counts[i].count ]);
  }
  this.render(xAxis, yAxis);
}

StackedBarTile.prototype.render = function (xAxis, yAxis) {
  var newDiv = this.newDiv;
  var object = this.object;
	var chartDiv = newDiv.find(".chart-item");
  var ctx = chartDiv.find("#"+object.id);
	ctx.width(ctx.width);
	ctx.height(230);
	$.plot(ctx, [xAxis], {
        series: {
            stack: true,
          bars: {
                show: true
            }
        },
        grid: {
            hoverable: true,
            color: "#B2B2B2",
            show: true,
            borderWidth: 1,
            borderColor: "#EEEEEE"
        },
    bars: {
            align: "center",
            horizontal: false,
            barWidth: .8,
            lineWidth: 0
        },
        xaxis: {
            mode: "time",
            timezone: "browser",
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: /*function(label, x, y) {
             var date = new Date(x);
             return label + ": " + y + " at " + date;
             }*/"%s: %y events at %x",
            defaultFormat: true
        }
    });
}
