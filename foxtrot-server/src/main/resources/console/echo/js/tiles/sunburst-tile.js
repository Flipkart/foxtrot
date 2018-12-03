/**
 * Copyright 2018 Phonepe Internet Pvt. Ltd.
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
function SunburstTile() {
    this.object = "";
}

function prepareNesting(array) {
    var nestingArray = [];
    $.each(array, function( index, value ) {
        nestingArray.push(currentFieldList[parseInt(value)].field);
    });
    return nestingArray;
}
  
  function getSunburstChartFormValues() {
    var nesting = $("#sunburst-nesting-field").val();
    var timeframe = $("#sunburst-timeframe").val();
    var period = $("#sunburst-time-unit").val();
    var unique = $("#sunburst-uniqueKey").val();
    if (nesting == "none") {
      return [[], false];
    }    
    return {
      "nesting": prepareNesting(nesting)
      , "timeframe": timeframe
      , "period": period
      , "uniqueCountOn": unique
    };
  }
  
  function setSunBurstChartFormValues(object) {
    var parentElement = $("#" + object.tileContext.chartType + "-chart-data");

    var nestingElement = [];
    $.each(object.tileContext.nesting, function( index, value ) {
        nestingElement.push(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[index])));
    });
    
    parentElement.find("#sunburst-nesting-field").val(nestingElement);

    $("#sunburst-nesting-field").selectpicker('refresh');
    
    parentElement.find("#sunburst-timeframe").val(object.tileContext.timeframe);
    
    parentElement.find("#sunburst-time-unit").val(object.tileContext.period);
    
    parentElement.find("#sunburst-time-unit").selectpicker('refresh');

    var uniqeKey = parentElement.find("#sunburst-uniqueKey");
    uniqeKey.val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueCountOn));
    $(uniqeKey).selectpicker('refresh');
  }
  
  function clearSunburstChartForm() {
    $('.sunburstForm')[0].reset();
    $(".sunburstForm").find('.selectpicker').selectpicker('refresh');
  }
  SunburstTile.prototype.getQuery = function (object) {
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
        showFetchError(refObject, "refresh");
      }
    });
  }
  SunburstTile.prototype.getData = function (data) {
    if(data.length == 0)
      showFetchError(this.object);
    else
      hideFetchError(this.object);
      
    this.object.tileContext.filters.pop();
    if (data.result == undefined || data.result.length == 0) return;
    var chartData = [];
    var object = {}
    for (var key in data.result) {
      object.axis = key;
      object.value = data.result[key]
      chartData.push({
        axis: key
        , value: data.result[key]
      });
    }
    this.render(chartData);
  }
  SunburstTile.prototype.render = function (data) {
    var a = [];
    a.push(data);
    var object = this.object;
    var d = a;
    var chartDiv = $("#"+object.id).find(".chart-item");
    var ctx = chartDiv.find("#radar-" + object.id);
    ctx.width(ctx.width);
    var mycfg = {
      color: function () {
        c = ['red', 'yellow', 'pink', 'green', 'blue', 'olive', 'aqua', 'cadetblue', 'crimson'];
        m = c.length - 1;
        x = parseInt(Math.random() * 100);
        return c[x % m]; //Get a random color
      }
      , w: 550
      , h: 300
    , }
    RadarChart.draw("#radar-" + object.id, d, mycfg);
  }
  