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
function MapTile() {
  this.object = "";
}

function getMapChartFormValues() {
  var period = $("#geo_aggregation-time-unit").val();
  var timeframe = $("#geo_aggregation-timeframe").val();
  var eventField = $("#geo_aggregation-event-field").val();
  var uniqueKey = $("#geo_aggregation-uniquekey").val();
  var ignoreDigits = $(".geo_aggregation-ignored-digits").val();
  var selectedValue = $("#geo_aggregation-selected-value").val();
  var aggregationType = $('#geo_aggregation-aggregation-type').val();
  var aggregationField = $('#geo_aggregation-aggregation-field').val();
  var latitude = parseFloat($("#geo_aggregation-latitude").val());
  var longitude = parseFloat($("#geo_aggregation-longitude").val());
  var zoom = parseFloat($("#geo_aggregation-zoom").val());
  var gridLevel = parseInt($("#geo_aggregation-gridLevel").val());


  if (eventField == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(eventField)].field;
  var status = true;

  if (uniqueKey == "none" || uniqueKey == "" || uniqueKey == null) {
    uniqueKey = null;
  } else {
    uniqueKey = currentFieldList[parseInt(uniqueKey)].field
  }


  if (aggregationField == "none" || aggregationField == "" || aggregationField == null || aggregationField == "undefined") {
    aggregationField = null;
  } else {
    aggregationField = currentFieldList[parseInt(aggregationField)].field
  }

  if (aggregationType == "none" || aggregationType == "" || aggregationType == "null" || aggregationType == "undefined") {
    aggregationType = null;
  }

  return {
    "period": period
    , "timeframe": timeframe,
    "locationField": groupingString
    , "nesting": [groupingString]
    , "uniqueKey": uniqueKey
    , "ignoreDigits": ignoreDigits
    , "selectedValue": selectedValue
    , "aggregationType": aggregationType
    , "aggregationField": aggregationField
    ,"gridLevel": gridLevel
    ,"latitude": latitude,
    "longitude": longitude,
    "zoom": zoom
  };
}

function setMapChartFormValues(object) {
  $("#geo_aggregation-time-unit").val(object.tileContext.period);
  $("#geo_aggregation-time-unit").selectpicker('refresh');
  $("#geo_aggregation-timeframe").val(object.tileContext.timeframe);
  $("#geo_aggregation-event-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#geo_aggregation-event-field").selectpicker('refresh');
  $("#geo_aggregation-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
  $("#geo_aggregation-uniquekey").selectpicker('refresh');
  $(".geo_aggregation-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  $("#geo_aggregation-selected-value").val((object.tileContext.selectedValue == undefined ? '' : object.tileContext.selectedValue));
  $('#geo_aggregation-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#geo_aggregation-aggregation-field').selectpicker('refresh');
  $('#geo_aggregation-aggregation-type').val();
  $("#geo_aggregation-latitude").val(object.tileContext.latitude);
  $("#geo_aggregation-longitude").val(object.tileContext.longitude);
  $("#geo_aggregation-zoom").val(object.tileContext.zoom);
  $("#geo_aggregation-gridLevel").val(object.tileContext.gridLevel);

}

function clearGeoAggregationChartForm() {
  $('.geo_aggregationForm')[0].reset();
  $(".geo_aggregationForm").find('.selectpicker').selectpicker('refresh');
}

MapTile.prototype.getQuery = function (object) {
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
    var requestTags = {
      "widget": this.object.title,
      "consoleId": getCurrentConsoleId()
    }
  var data = {
    "opcode": "geo_aggregation"
    , "consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    ,"locationField": object.tileContext.locationField
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    , "aggregationField": object.tileContext.aggregationField
    , "aggregationType": object.tileContext.aggregationType,
    "gridLevel": object.tileContext.gridLevel,
    "sourceType":"ECHO_DASHBOARD",
          "requestTags": requestTags,
    
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
MapTile.prototype.getData = function (data) {
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }
  
  var columns = [];
  this.object.tileContext.uiFiltersList = [];
  for (property in data.metricByGrid) {
    var visible = $.inArray( property, this.object.tileContext.uiFiltersSelectedList);
    if ((visible == -1 ? true : false)) {
      columns.push({
        COORDINATES: [parseInt(property.split(",")[1].trim()), parseInt(property.split(",")[0])],
        weight: data.metricByGrid[property]
      });
    }
    this.object.tileContext.uiFiltersList.push(property);
  }
  
  this.render(columns, Object.keys({}).length)
}

MapTile.prototype.render = function (columns, dataLength) {
  $("#"+this.object.id).find(".chart-item").find("#"+this.object.id).height('600px')
  $("#"+this.object.id).find(".chart-item").css('margin-top', '35px')
  console.log("Columns", columns);
  new deck.DeckGL({
    container: $("#"+this.object.id).find(".chart-item").find("#"+this.object.id)[0],
    mapStyle: 'https://basemaps.cartocdn.com/gl/positron-nolabels-gl-style/style.json',
    initialViewState: {
      longitude: this.object.tileContext.longitude || 77.45,
      latitude: this.object.tileContext.latitude || 12.34, 
      zoom: this.object.tileContext.zoom || 3,
    },
    controller: true,
    layers: [
      new deck.HeatmapLayer({
        data: columns,
        getPosition: d => d.COORDINATES,
        getWeight: d => d.weight,
        radius: 1,
        threshold: 0.03,
        intensity: 0.8,
        radiusPixels: 20
      })
    ],
    onViewStateChange: ({viewState}) => {
      this.object.tileContext.latitude = viewState.latitude;
      this.object.tileContext.longitude = viewState.longitude;
      this.object.tileContext.zoom = viewState.zoom;
    }
  });
  
}
