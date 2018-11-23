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

function CountTile() {
  this.object = "";
}

function getCountChartFormValues() {
  var period = $("#count-time-unit").val();
  var statsField = $("#count-field").val();
  var timeframe = $("#count-timeframe").val();
  var ignoreDigits = $(".count-ignored-digits").val();
  var isDistinct = $("#count-dist").is(":checked");
  var status = true;

  if(period == "none" || timeframe == "") {
    return[[], false];
  }

  if(statsField && statsField != "none") {
    statsField = currentFieldList[parseInt(statsField)].field;
  } else {
    statsField = null;
  }

  return {
    "period": period,
    "field": statsField,
    "timeframe": timeframe
    , "ignoreDigits" : ignoreDigits
    , "isDistinct": isDistinct
  };
}

function clearCountChartForm() {
  $(".countForm").find('#count-dist').attr('checked', false);  
  $('.countForm')[0].reset();
  $(".countForm").find('.selectpicker').selectpicker('refresh');
}

function setCountChartFormValues(object) {
  
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find("#count-time-unit");
  timeUnitEl.val(object.tileContext.period);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find("#count-field");
  var statsFieldIndex = currentFieldList.findIndex(x => x.field== object.tileContext.field);
  statsFieldEl.val(parseInt(statsFieldIndex));
  $(statsFieldEl).selectpicker('refresh');

  parentElement.find("#count-timeframe").val(object.tileContext.timeframe);
  parentElement.find(".count-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  parentElement.find("#count-dist").val(object.tileContext.isDistinct == undefined ? parentElement.find("#count-dist").attr('checked', false) : parentElement.find("#count-dist").attr('checked', object.tileContext.isDistinct)) ;
}

CountTile.prototype.getQuery = function(object) {
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

  var data = {};

  if(object.tileContext.field && object.tileContext.field != "none") {
    data = {
      "opcode": "count",
      "table": object.tileContext.table,
      "filters": filters,
      "field": object.tileContext.field && object.tileContext.field != "none" ? object.tileContext.field : null,
      "distinct": object.tileContext.isDistinct == undefined ? false : object.tileContext.isDistinct
    }
  } else {
    data = {
      "opcode": "count",
      "table": object.tileContext.table,
      "filters": filters
    }
  }
  var refObject = this.object;
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
    ,error: function(xhr, textStatus, error) {
      showFetchError(refObject);
    }
  });
}

CountTile.prototype.getData = function(data) {

  if(!data)
    showFetchError(this.object);
  else
    hideFetchError(this.object);
    
  if(!data)
    return;
  this.render(data.count);
}

CountTile.prototype.render = function (displayValue) {
  var object = this.object;
  var chartDiv = $("#"+object.id).find(".chart-item");
  chartDiv.addClass("trend-chart");

  var a = chartDiv.find("#"+object.id);
  if(a.length != 0) {
    a.remove();
  }

  displayValue = displayValue / Math.pow(10, (this.object.tileContext.ignoreDigits == undefined ? 0 : this.object.tileContext.ignoreDigits));
  chartDiv.append("<div id="+object.id+"><p class='trend-value-big bold'>"+numberWithCommas(displayValue)+"</p><dhr/><p class='trend-value-small'></p><div id='trend-'"+object.id+" class='trend-chart-health'></div><div class='trend-chart-health-percentage bold'></div></div>");
}
