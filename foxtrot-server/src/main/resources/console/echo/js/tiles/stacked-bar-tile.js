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
  this.object = "";
}

function getstackedBarChartFormValues() {
  var period = $("#stacked-bar-time-unit").val();
  var timeframe = $(".stacked-bar-timeframe").val();
  var chartField = $("#stacked-bar-field").val();
  var uniqueKey = $("#stacked-bar-uniquekey").val();
  var multiSeries = $("#stacked-bar-multiple-series-value").val();

  var ignoreDigits = $(".stackedBar-ignored-digits").val();

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
    , "stackedBarField": chartField
    , "ignoreDigits" : ignoreDigits
    , "multiSeries": parseInt(multiSeries)};
}

function setStackedBarChartFormValues(object) {
  $("#stacked-bar-time-unit").val(object.tileContext.period);
  $("#stacked-bar-time-unit").selectpicker('refresh');
  $("#stacked-bar-timeframe").val(object.tileContext.timeframe);
  $("#stacked-bar-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.stackedBarField)));
  $("#stacked-bar-field").selectpicker('refresh');
  $("#stacked-bar-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
  $("#stacked-bar-uniquekey").selectpicker('refresh');
  $(".stackedBar-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));

  var parentElement = $("#"+currentChartType+"-chart-data");
  var multiSeries = parentElement.find("#stacked-bar-multiple-series-value");
  var multiSeriesValue = (object.tileContext.multiSeries == undefined ? "" : object.tileContext.multiSeries)
  multiSeries.val(parseInt(multiSeriesValue));
  $(multiSeries).selectpicker('refresh');
}

function clearStackedBarChartForm() {
  $('.stackedBarForm')[0].reset();
  $(".stackedBarForm").find('.selectpicker').selectpicker('refresh');
}

StackedBarTile.prototype.getQuery = function (object) {
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
    "opcode": "multi_query"
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "field": object.tileContext.stackedBarField
    , period: periodFromWindow(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
  }

  var multiQueryData = {};
  var multiSeiresValue = object.tileContext.multiSeries;
  if((multiSeiresValue != undefined) && (multiSeiresValue != "") && (multiSeiresValue > 1)) {
    multiQueryData["requests"] = prepareMultiSeriesQueryObject(data, object, filters);
    multiQueryData["opcode"] = "multi_query";
  } else {
    data["opcode"] = "trend";
    multiQueryData = data;
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
    , data: JSON.stringify(multiQueryData)
    , success: $.proxy(this.getData, this)
    ,error: function(xhr, textStatus, error) {
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
    }
  });
}

// prepare data for individual lines
function prepareCusotmdata(obj) {
  var data = [];
  for(var rhs = 0;  rhs < obj.length; rhs++) {
    data.push([obj[rhs].period, obj[rhs].count]);
  }
  return data;
}

StackedBarTile.prototype.getData = function (data) {

  if(data.length == 0)
    showFetchError(this.object);
  else
    hideFetchError(this.object);

  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }

  var originalSeries = [];
  var isMultiSeries = false;
  var multiTotalResponseArray = [];
  if(!data.trends) { // if multi series line present prepare data similar to normal lines
    isMultiSeries = true;
    var tmpArray = {};
    var originalTmpArray = {}; // dummy object to keep extra information on original data
    var tmpData = data.responses;
    var index = 0;
    var zeroIndex = [];
    for(var tmpValue in tmpData) {
      if (tmpData.hasOwnProperty(tmpValue)) {
        var obj = tmpData[tmpValue].trends;
        multiTotalResponseArray.push(obj)
        for(var k in obj) {
          if(obj.hasOwnProperty(k)) {
            
            if(index == 0)
              zeroIndex = obj[k];
            
            // add unique identification to draw the line
            var lhs = k+" ("+(index+1)+") "+readableShortDate(obj[k][0].period);
            
              if(index == 0) {
              tmpArray[lhs] = obj[k];
              originalTmpArray[lhs] = prepareCusotmdata(obj[k]);
            } else {
              //except first line replace x value to first data line to remaining lines to plot graph
              var newRhs = [];
              var or = [];
              for(var rhs = 0; rhs < obj[k].length; rhs++) {
                if(zeroIndex[rhs]) {
                  newRhs.push({"period":zeroIndex[rhs].period, "count": obj[k][rhs].count})
                  
                }
                if(obj[k][rhs]) {
                  or.push([obj[k][rhs].period, obj[k][rhs].count]);
                }
              }
              tmpArray[lhs] = newRhs;
              originalTmpArray[lhs] = or;
            }
          }
        }
        index++;
     }
    }
    data.trends = tmpArray;
    originalSeries = originalTmpArray;
  }
  
  var colors = new Colors(Object.keys(data.trends).length);
  var d = [];
  var colorIdx = 0;
  var timestamp = new Date().getTime();
  var tmpData = new Object();
  var regexp = null;

  

  for (var trend in data.trends) {
    if (regexp && !regexp.test(trend)) {
      continue;
    }
    var trendData = data.trends[trend];
    for (var i = 0; i < trendData.length; i++) {
      var time = trendData[i].period;
      var count = trendData[i].count / Math.pow(10, (this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits));
      if (!tmpData.hasOwnProperty(time)) {
        tmpData[time] = new Object();
      }
      tmpData[time][trend] = count;
    }
  }

  var trendWiseData = new Object();
  for (var time in tmpData) {
    for (var trend in data.trends) {
      if (regexp && !regexp.test(trend)) {
        continue;
      }
      var count = 0;
      var timeData = tmpData[time];
      if (timeData.hasOwnProperty(trend)) {
        count = timeData[trend];
      }
      var rows = null;
      if (!trendWiseData.hasOwnProperty(trend)) {
        rows = [];
        trendWiseData[trend] = rows;
      }
      rows = trendWiseData[trend];
      var timeVal = parseInt(time);
      rows.push([timeVal, count]);
    }
  }
  this.object.tileContext.uiFiltersList = [];
  var originalData = [];

  for (var trend in trendWiseData) {
    var rows = trendWiseData[trend];
    if (regexp && !regexp.test(trend)) {
      continue;
    }
    rows.sort(function (lhs, rhs) {
      return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0]) ? 0 : 1);
    })
    var visible = $.inArray( trend, this.object.tileContext.uiFiltersSelectedList);
    if((visible == -1 ? true : false)) {
      var commonColor = colors.nextColor();
      d.push({
        data: rows
        , color: convertHex(commonColor, 100)
        , label: trend
        , fill: 0.3
        , fillColor: "#A3A3A3"
        , lines: {
          show: true
        },
        points:{show: (rows.length <= 50 ? true :false), radius : 3.5}
        , shadowSize: 0 /*, curvedLines: {apply: true}*/
      });
      if(isMultiSeries) { // have same data similar to normal one
        originalData.push({
          data: originalSeries[trend]
          , color: convertHex(commonColor, 100)
          , label: trend
          , fill: 0.3
          , fillColor: "#A3A3A3"
          , lines: {
            show: true
          },
          points:{show: (rows.length <= 50 ? true :false), radius : 3.5}
          , shadowSize: 0 /*, curvedLines: {apply: true}*/
        });
      }
    }
    this.object.tileContext.uiFiltersList.push(trend);
  }

  // var multiTotal = [];
  // if(!isMultiSeries) {
  //   var allValues = _.values(trendWiseData);
  //   var singleValue = allValues[0];
  //   var allLength = allValues.length;
  //   for(var i = 0; i < singleValue.length; i++) {
  //     var total = allValues[0][i][1];
  //     for(var j = 1; j < allLength; j++) {
  //       total+= allValues[j][i][1];
  //     }
  //     multiTotal.push([allValues[0][i][0],total])
  //     total = 0;
  //   }
  //   d.splice(0, 0,{
  //     data: multiTotal
  //     , color: convertHex("#33CAFF", 100)
  //     , label: "Total"
  //     , fill: 0.3
  //     , fillColor: "#A3A3A3"
  //     , lines: {
  //       show: true
  //     },
  //     points:{show: (multiTotal.length <= 50 ? true :false), radius : 3.5}
  //     , shadowSize: 0 /*, curvedLines: {apply: true}*/
  //   })
  // } else {
  //   var finalArray = [];
  //   for(var response in multiTotalResponseArray) {
  //     var multiValue = [];
  //     if(multiTotalResponseArray.hasOwnProperty(response)) {
  //       var final = multiTotalResponseArray[response];
  //       console.log(final)
  //       var iteration = Object.keys(final).length;
  //       var allValues = Object.values(final);
  //       for(var i = 0; i < allValues[0].length; i++){ // loop single
  //         var total = allValues[0][i]["count"];
  //         for(var j = 1; j < allValues.length; j++) { // loop all except first index
  //           total+= allValues[j][1]["count"];
  //         }
  //         multiValue.push([d[0].data[i][0], total]);// d[0] - is sorted time periods
  //         total = 0;
  //       }
  //     }
  //     finalArray.push(multiValue)
  //   }

  //   var colors = new Colors(finalArray.length);
  //   for(var n = 0; n < finalArray.length; n++) {
  //     var totalColor = colors.nextColor();
  //     d.splice(0, 0, {
  //       data: finalArray[n]
  //       , color: convertHex(totalColor, 100)
  //       , label: "Series "+(n+1)+" Total"
  //       , fill: 0.3
  //       , fillColor: "#A3A3A3"
  //       , lines: {
  //         show: true
  //       },
  //       points:{show: (finalArray[n].length <= 50 ? true :false), radius : 3.5}
  //       , shadowSize: 0 /*, curvedLines: {apply: true}*/
  //     });
  //     originalData.splice(0, 0,{
  //       data: finalArray[n]
  //       , color: convertHex(totalColor, 100)
  //       , label: "Series "+(n+1)+" Total"
  //       , fill: 0.3
  //       , fillColor: "#A3A3A3"
  //       , lines: {
  //         show: true
  //       },
  //       points:{show: (finalArray[n].length <= 50 ? true :false), radius : 3.5}
  //       , shadowSize: 0 /*, curvedLines: {apply: true}*/
  //     })
  //   }
  // }
  this.render(d,isMultiSeries, originalData);
}
StackedBarTile.prototype.render = function (d, isMultiSeries, originalData) {

  if(d.length == 0)
    showFetchError(this.object, "data", null);
  else 
    hideFetchError(this.object);

  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  var borderColorArray = ["#9e8cd9", "#f3a534", "#9bc95b", "#50e3c2"]
  var ctx = chartDiv.find("#" + object.id);
  var chartClassName = object.tileContext.widgetSize == undefined ? getFullWidgetClassName(12) : getFullWidgetClassName(object.tileContext.widgetSize);
  ctx.addClass(chartClassName);
  $("#"+object.id).find(".chart-item").find(".legend").addClass('full-widget-legend');
  //$("#"+object.id).find(".chart-item").css('margin-top', "53px");
  ctx.width(ctx.width);
  ctx.height(fullWidgetChartHeight());

  if(d.length == 0) {
    ctx.hide();
    $(chartDiv.find(".legend")).hide();
  } else {
    $(chartDiv.find(".legend")).show();
    ctx.show();
    var plot = $.plot(ctx, d, {
      series: {
        stack: true
        , lines: {
          show: true
          , fill: false
          , lineWidth: 1.0
          , fillColor: {
            colors: [{
              opacity: 1
                      }, {
              opacity: 0.5
                      }]
          }
        }
        , shadowSize: 0
        , curvedLines: { active: true }
      }
      ,crosshair: {
      mode: "x"
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
      }
      , yaxis: {
        tickFormatter: function(val, axis) {
          return numDifferentiation(val);
        },
      }
      , xaxis: {
        mode: "time"
        , timezone: "browser"
        , timeformat: axisTimeFormat(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)))
        , tickLength: 0
      , }
      , selection: {
        mode: "x"
        , minSize: 1
      }
      , tooltip: false
      , tooltipOpts: {
        content: "%y events at %x"
        , defaultFormat: true
      }
      , legend: {
        show: false
      }
      ,highlightSeries: {
        color: "#FF00FF"
      }
    });
  
    drawStackedLinesLegend(d, $(chartDiv.find(".legend")));
  
    // Series point for every time
  //  var updateLegendTimeout = null;
  //  var latestPosition = null;
  //  function updateLegend() {
  //    updateLegendTimeout = null;
  //    var pos = latestPosition;
  //    var axes = plot.getAxes();
  //    if (pos.x < axes.xaxis.min || pos.x > axes.xaxis.max ||
  //        pos.y < axes.yaxis.min || pos.y > axes.yaxis.max) {
  //      return;
  //    }
  //
  //    var i, j, dataset = plot.getData();
  //    var total = 0;
  //    var series, globalX ,globalY;
  //    $("#tooltip").remove();
  //    var strTip = "";
  //    var strTipInsideRows = "";
  //    if(dataset) {
  //      for (i = 0; i < dataset.length; ++i) {
  //        var series = dataset[i];
  //        for (j = 0; j < series.data.length; ++j) {
  //          if (series.data[j][0] > pos.x) {
  //            break;
  //          }
  //        }
  //
  //        //DD MMM HH:mm ss
  //        var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
  //        strTip = "<table border='1' class='stacked-tooltip'><tr><td class='tooltip-table-first-td' colspan='2'>"+moment(pos.x).format(a)+"</td>";
  //        var y,x,
  //            p1 = series.data[j - 1],
  //            p2 = series.data[j];
  //
  //        if (p1 == null) {
  //          y = p2[1];
  //          x = p2[0];
  //        } else if (p2 == null) {
  //          y = p1[1];
  //          x = p1[0]
  //        } else {
  //          y = p1[1] + (p2[1] - p1[1]) * (pos.x - p1[0]) / (p2[0] - p1[0]);
  //          x = p1[0]
  //          console.log('=='+p1[1])
  //        }
  //
  //        var o = plot.pointOffset({
  //          x: pos.x,
  //          y: y
  //        });
  //
  //        //console.log(x);
  //        total = total+parseFloat(y);
  //        strTipInsideRows += "<tr><td class='tooltip-text'>"+series.label+ "</td>" + "<td class='tooltip-count' style='color:"+series.color+"'>"+numberWithCommas(y.toFixed(0)) + '</td></tr>';
  //        strTip =  strTip+strTipInsideRows+"<tr><td class='tooltip-text'><b>TOTAL</b></td> <td style='color:#42b1f7' class='tooltip-count'>"+numberWithCommas(total.toFixed(0))+"</td></tr></table>" ;
  //        globalX = pos.pageX;
  //        globalY = pos.pageY;
  //      }
  //      showTooltip(globalX, globalY, strTip, "", ctx);
  //    }else {
  //      $("#tooltip").remove();
  //    }
  //  }
  //
  //  $(ctx).bind("plothover",  function (event, pos, item) {
  //    latestPosition = pos;
  //    if (!updateLegendTimeout) {
  //      $("#tooltip").remove();
  //      updateLegendTimeout = setTimeout(updateLegend, 50);
  //    }
  //    $("#tooltip").remove();
  //  });
  //
    function showTooltip(x, y, contents, color, ctx) {
     var tooltip =  $('<div id="tooltip">' + contents + '</div>').css({
        position: 'absolute',
        display: 'block',
        top: y + 5,
        left: x + 5,
        'background-color': '#fff',
        'box-shadow': '0 2px 4px 0 #cbd7e9',
        'z-index': 5000,
        'line-height': 2
      }).appendTo("body").fadeOut(10000);
  
      // stop fadeout
      $(tooltip).mouseenter(
        function () {
          if($(this).is(':animated')) {
             $(this).stop( true, true ).fadeIn();
          }
        }
      );
  
      // remove tooltip when user leaves the mousehover
      $(tooltip).mouseleave(function() {
        $(this).remove();
      });
  
      var closeEl = $(tooltip).find(".close-tooltip");// find tooltip elemetn
      $(closeEl).click(function() { // add click event
        $(tooltip).hide();
      });
  
      $(tooltip).width($(".stacked-tooltip tbody").width());
      $(".stacked-tooltip thead").width($(".stacked-tooltip tbody").width());
      $(".stacked-tooltip tfoot").width($(".stacked-tooltip tbody").width());
      // adjust position of tooltip
      var width = $("#tooltip").width();
      var height = $("#tooltip").height();
      if(x > 900 && width > 300) {
        $("#tooltip").css({"left": x - width});
      }
  
      var topPosition = Math.abs($("#"+$(ctx).attr('id')).offset().top);
  
      if(height <= 200) {
        $("#tooltip").css({"top" : y});
      }
      else if(topPosition <= 100) {
        $("#tooltip").css({"top" : topPosition});
      } else {
        $("#tooltip").css({"top" : topPosition - 40});
      }
    }
  
    var previousPoint = null;
    $(ctx).bind("plothover", function (event, pos, item) {
      if (item) {
        $("#tooltip").remove();
        var hoverSeries = item.series; // what series am I hovering?
        var x = item.datapoint[0],
            y = item.datapoint[1];
        var color = item.series.color;
  
        var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
        var strTip = "<table border='1' class='stacked-tooltip'><thead><tr><td class='tooltip-table-first-td' colspan='2'>"+moment(x).format(a)+"</td><td class='tooltip-table-first-td' colspan='2'><span class='close-tooltip'>Close</span></td></tr></thead>"; // start string with current hover
        var total = 0;
        var strTipInsideRows = "";
        var allSeries = plot.getData();
        for (var i = allSeries.length - 1; i >= 0; i--) {
          var data = allSeries[i].data;
          $.each(data, function(j,p){
            if (p[0] == x){  // if my hover x == point x add to string
              total = total +p[1];
              strTipInsideRows += "<tr><td class='tooltip-text'>"+allSeries[i].label+ "</td>" + "<td class='tooltip-count' style='color:"+allSeries[i].color+"'>"+numberWithCommas(p[1]) + '</td></tr>';
            }
            else {
              $("#tooltip").remove();
              previousPoint = null;
            }
          });
        }
        strTip =  strTip+strTipInsideRows+"<tfoot><tr><td class='tooltip-text'><b>TOTAL</b></td> <td style='color:#42b1f7' class='tooltip-count tooltip-total'>"+numberWithCommas(total)+"</td></tr></tfoot></table>"
        showTooltip(item.pageX, item.pageY, strTip, color, ctx);
      }
    });
  
  
    function individualTooltip(x, y, xValue, yValue) {
      var a = axisTimeFormatNew(object.tileContext.period, (globalFilters ? getGlobalFilters() : getPeriodSelect(object.id)));
      $('<div id="flot-custom-tooltip" class="custom-tooltip"> <div class="tooltip-custom-content"><p class="">'+numDifferentiation(yValue)+'</p><p class="tooltip-custom-date-text">' + moment(xValue).format(a) + '</p></div></div>').css({
        position: 'absolute',
        display: 'none',
        top: y,
        left: x,
      }).appendTo("body").fadeIn(200);
    }
  
    var re = re = /\(([0-9]+,[0-9]+,[0-9]+)/;
    $(chartDiv.find('.legend ul li')).on('mouseenter', function() {
      var label = $(this).text();
      var points = plot.getData();
      var graphx = ctx.offset().left;
      graphx = graphx + 30; // replace with offset of canvas on graph
      var graphy = ctx.offset().top;
      graphy = graphy + 10; // how low you want the label to hang underneath the point
      var currentIndex = 0;
      for(var k = 0; k < points.length; k++){
        currentIndex = $(this).index();
        if(points[k].label== $.trim(label)) {
          for(var m = 0; m < points[k].data.length; m++){
            if(isMultiSeries) { // muli series and change x and y value by using origital data variable
              var realValue = originalData[$(this).index()];
              if(realValue.data[m]) {
                individualTooltip(graphx + points[k].xaxis.p2c(points[k].data[m][0]), points[k].yaxis.p2c(points[k].data[m][1]) + graphy - 150, realValue.data[m][0], points[k].data[m][1]);
              }
            } else {
              individualTooltip(graphx + points[k].xaxis.p2c(points[k].data[m][0]), points[k].yaxis.p2c(points[k].data[m][1]) + graphy - 70, points[k].data[m][0], points[k].data[m][1]);
            }
          }
          points[k].oldColor = points[k].color;
          points[k].color = 'rgba(' + re.exec(points[k].color)[1] + ',' + 1 + ')'; 
        } else {
          currentValue = [];          
          points[k].color = 'rgba(' + re.exec(points[k].color)[1] + ',' + 0.1 + ')';
        }
      }
      // if multi series set data and upgrade grid for new x and y    
      if(isMultiSeries) {
        plot.setData([originalData[currentIndex]]); 
        plot.setupGrid();
      }
      plot.draw();
    });
  
    $(chartDiv.find('.legend ul li')).on('mouseleave', function() {
      var label = $(this).text();
      var allSeries = plot.getData();
      $("#flot-custom-tooltip").remove();
      for (var i = 0; i < allSeries.length; i++){
        allSeries[i].color = 'rgba(' + re.exec(allSeries[i].color)[1] + ',' + 1 + ')';
        $(".custom-tooltip").remove();
        $(".data-point-label").remove();
      }
      plot.setData(d);    
      plot.setupGrid();
      plot.draw();
    });
  }
}
