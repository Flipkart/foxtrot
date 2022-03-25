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
function FunnelTile() {
    this.object = "";
    this.analyticsData={}
  }
  
  function getFunnelChartFormValues() {
    var period = $("#funnel-time-unit").val();
    var timeframe = $(".funnel-timeframe").val();
    var chartField = $("#funnel-field").val();
    var uniqueKey = $("#funnel-uniquekey").val();
    var funnelId = $("#funnel-id").val();
    var aggregationType = $('#funnel-aggregation-type').val();
    var aggregationField = $('#funnel-aggregation-field').val();
    var extrapolationTemp = $('#mark-extrapolation-funnel-chart').is(":checked");
    
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
  
    if (chartField == "none") {
      return [[], false];
    }
    chartField = currentFieldList[parseInt(chartField)].field;
    return {
      "period": period
      , "timeframe": timeframe
      , "uniqueKey": uniqueKey
      , "funnelField": chartField
      , "funnelId": funnelId
      , "aggregationType":aggregationType
      , "aggregationField":aggregationField
      , "extrapolationTemp":extrapolationTemp};
  }
  
  function setFunnelChartFormValues(object) {
    $("#funnel-time-unit").val(object.tileContext.period);
    $("#funnel-time-unit").selectpicker('refresh');
    $("#funnel-timeframe").val(object.tileContext.timeframe);
    $("#funnel-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.funnelField)));
    $("#funnel-field").selectpicker('refresh');
    $("#funnel-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
    $("#funnel-uniquekey").selectpicker('refresh');
    $("#funnel-id").val(object.tileContext.funnelId)
    $('#funnel-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
    $('#funnel-aggregation-field').selectpicker('refresh');
    $('#funnel-aggregation-Type').val();
    $('#mark-extrapolation-funnel-chart').prop("checked",object.tileContext.extrapolationTemp).change();

    console.log("eventtype........",object.tileContext.funnelField);
    console.log("x.field........",object.tileContext.field);
    console.log("object....",object);
    console.log("currentFieldList.....",currentFieldList)
  }
  
  function clearfunnelChartForm() {
    $('.funnelForm')[0].reset();
    $(".funnelForm").find('.selectpicker').selectpicker('refresh');
  }


  FunnelTile.prototype.getQuery = function (object) {
    this.object = object;
    var filters = [];
    todayTomorrow(
      filters,
      globalFilters,
      getGlobalFilters,
      getPeriodSelect,
      timeValue,
      object
    );

    if(object.tileContext.filters) {
      for (var i = 0; i < object.tileContext.filters.length; i++) {
        filters.push(object.tileContext.filters[i]);
      }
    }
  
    if (object.tileContext.selectedValue) {
      filters.push({
        field: object.tileContext.nesting.toString(),
        operator: "in",
        values: object.tileContext.selectedValue.split(',')
      });
    }
  
    var templateFilters = isAppendTemplateFilters(object.tileContext.table);
    if(templateFilters.length > 0) {
      filters = filters.concat(templateFilters);
    }
    
     let funnelfilter = {
      "operator": "equals",
			"field": "eventData.funnelInfo.funnelId",
      "value":object.tileContext.funnelId
    }

    const extrapolationTemp = $('#mark-extrapolation-funnel-chart').is(":checked");
    var requestTags = {
      "widget": this.object.title,
      "consoleId":getCurrentConsoleId()
    }

    var data = {
      "opcode": "group"
      ,"consoleId": getCurrentConsoleId()
      , "table": object.tileContext.table
      , "filters": filters.concat(funnelfilter)
      , "nesting":  [object.tileContext.funnelField]
      ,"aggregationField":object.tileContext.aggregationField
      ,"aggregationType":object.tileContext.aggregationType
      , "extrapolationFlag": extrapolationTemp
      ,"sourceType":"ECHO_DASHBOARD"
      ,"requestTags": requestTags
    }
    $.ajax({
      method: "post"
      , dataType: 'json'
      , accepts: {
        json: 'application/json'
      }
      , url: apiUrl + "/v2/analytics"
      , contentType: "application/json"
      , data: JSON.stringify(data)
      , success: $.proxy(this.orderTypeApi, this)
      ,error: function(xhr, textStatus, error) {
        showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
      }
    });
  }

  FunnelTile.prototype.orderTypeApi = function (analyticsData) {
    this.analyticsData = analyticsData
    $.ajax({
      type: 'GET',
      url: apiUrl + `/funnel/${this.object.tileContext.funnelId}`,
      success: $.proxy(this.render, this),
      error: function(xhr, textStatus, error) {
        showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
      }
  });
  }

  FunnelTile.prototype.render = function (tileData) {
  var sortedData = []
  var isThere = false;
  var isInFilter = true;
  var isInFilterTemp = true;
  const { uiFiltersSelectedList, uiFiltersList} = this.object.tileContext
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }
  this.object.tileContext.uiFiltersList = [];
  if (uiFiltersSelectedList) {
    tileData.eventAttributes.forEach((sortData) => {
      this.analyticsData && Object.keys(this.analyticsData.result).forEach((data) => {
        uiFiltersSelectedList.forEach((filterData) => {
          if(filterData === sortData.eventType) {
            isInFilter = false;
            isInFilterTemp = false;
          } 
        })
        if(isInFilter && sortData.eventType === data) {
          sortedData.push({'label': data, 'value': this.analyticsData.result[data]});
          isInFilterTemp=false;
        }
        isInFilter = true;
      })
      if(isInFilterTemp) {
        sortedData.push({'label': sortData.eventType, 'value': 0});
      }
      isInFilterTemp = true;
      this.object.tileContext.uiFiltersList.push(sortData.eventType);
    })
  } else {
    tileData.eventAttributes.forEach((sortData) => {
      this.analyticsData && Object.keys(this.analyticsData.result).forEach((data) => {
        if(sortData.eventType === data){
          sortedData.push({'label': data, 'value': this.analyticsData.result[data]})
          isThere = true;
        }
    })
    !isThere && sortedData.push({'label': sortData.eventType, 'value': 0})
    isThere = false;
    this.object.tileContext.uiFiltersList.push(sortData.eventType);
})

  }
  if( uiFiltersSelectedList && uiFiltersList.length === uiFiltersSelectedList.length && uiFiltersSelectedList.length !== 0) {
    showFetchError(this.object, "data", '');
  }
  const data = sortedData

  const options = {
      block: {
          dynamicHeight: true,
          minHeight: 15,
      },
      height: 100,
      width: 100
  };
  const tagColor = ["#1f77b4","#ff7f0d","#2da02c","#d62728","#9467bd","#8d564b","#e377c2","#7f7f7f","#bcbd21","#bcbd21"]
  const colorData = data.map((datas, index) => {
      datas.backgroundColor = tagColor[index % tagColor.length]
      return datas
  })
var object = this.object;
var chartDiv = $("#"+object.id).find(".chart-item");
var ctx = chartDiv.find("#radar-" + object.id);
ctx.width(ctx.width);
$("#funnel-" + object.id).css("width", 500);
$("#funnel-" + object.id).css("height", 300);
  const chart = new D3Funnel("#funnel-" + object.id);
  FunnelWidget.widgetFilterChange(this.object.tileContext.uiFiltersList)
  chart.draw(colorData, options);
  }


  // -----------------------DownloadWidget---Starts---------------

  FunnelTile.prototype.downloadWidget = function (object) {
    this.object = object;
    var filters = [];
    todayTomorrow(
      filters,
      globalFilters,
      getGlobalFilters,
      getPeriodSelect,
      timeValue,
      object
    );

    if(object.tileContext.filters) {
      for (var i = 0; i < object.tileContext.filters.length; i++) {
        filters.push(object.tileContext.filters[i]);
      }
    }
  
    if (object.tileContext.selectedValue) {
      filters.push({
        field: object.tileContext.nesting.toString(),
        operator: "in",
        values: object.tileContext.selectedValue.split(',')
      });
    }
  
    var templateFilters = isAppendTemplateFilters(object.tileContext.table);
    if(templateFilters.length > 0) {
      filters = filters.concat(templateFilters);
    }
    
     let funnelfilter = {
      "operator": "equals",
			"field": "eventData.funnelInfo.funnelId",
      "value":object.tileContext.funnelId
    }

    var data = {
      "opcode": "group"
      ,"consoleId": getCurrentConsoleId()
      , "table": object.tileContext.table
      , "filters": filters.concat(funnelfilter)
      , "nesting":  [object.tileContext.funnelField]
      ,"aggregationField":object.tileContext.aggregationField
      ,"aggregationType":object.tileContext.aggregationType
    }
    $.ajax({
      url: apiUrl + "/v1/analytics/download",
      type: 'POST',
      data: JSON.stringify(data),
      dataType: 'text',
  
      contentType: 'application/json',
      context: this,
      success: function(response) {
        downloadTextAsCSV(response, 'Funnel.csv')
      },
      error: function(xhr, textStatus, error ) {
      }
  });
  }
  // -----------------------DownloadWidget---Endss---------------
