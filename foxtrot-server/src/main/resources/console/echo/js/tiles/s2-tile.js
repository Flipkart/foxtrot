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
function S2GridTile() {
  this.object = "";
}

function getS2gridFormValues() {
  var period = $("#s2grid-time-unit").val();
  var timeframe = $("#s2grid-timeframe").val();
  var eventField = $("#s2grid-event-field").val();
  var uniqueKey = $("#s2grid-uniquekey").val();
  var ignoreDigits = $(".s2grid-ignored-digits").val();
  var selectedValue = $("#s2grid-selected-value").val();
  var quantile = $('.s2grid-color-quantile').is(':checked');
  var aggregationType = $('#s2grid-aggregation-type').val();
  var aggregationField = $('#s2grid-aggregation-field').val();
  var latitude = parseFloat($("#s2grid-latitude").val());
  var longitude = parseFloat($("#s2grid-longitude").val());
  var zoom = parseFloat($("#s2grid-zoom").val());


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
    , "timeframe": timeframe
    , "nesting": [groupingString]
    , "uniqueKey": uniqueKey
    , "ignoreDigits": ignoreDigits
    , "selectedValue": selectedValue
    , "quantile": quantile
    , "aggregationType": aggregationType
    , "aggregationField": aggregationField,
    "latitude": latitude,
    "longitude": longitude,
    "zoom": zoom
  };
}

function setS2GridFormValues(object) {
  console.log(object.tileContext.aggregationType);
  $("#s2grid-time-unit").val(object.tileContext.period);
  $("#s2grid-time-unit").selectpicker('refresh');
  $("#s2grid-timeframe").val(object.tileContext.timeframe);
  $("#s2grid-event-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#s2grid-event-field").selectpicker('refresh');
  $("#s2grid-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
  $("#s2grid-uniquekey").selectpicker('refresh');
  $(".s2grid-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  $("#s2grid-selected-value").val((object.tileContext.selectedValue == undefined ? '' : object.tileContext.selectedValue));
  $('#s2grid-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#s2grid-aggregation-field').selectpicker('refresh');
  $('#s2grid-aggregation-type').val();
  $("#s2grid-latitude").val(object.tileContext.latitude);
  $("#s2grid-longitude").val(object.tileContext.longitude);
  $("#s2grid-zoom").val(object.tileContext.zoom);

  // ------  start for checkbox set values -------
  if (object.tileContext.quantile === true) {
    $("#s2grid-color-quantile").prop("checked", true);
  }
  else {
    $("#s2grid-color-quantile").val((object.tileContext.quantile == undefined ? 'undefined' : object.tileContext.quantile));
  }
}

function clearS2gridForm() {
  $('.s2gridForm')[0].reset();
  $(".s2gridForm").find('.selectpicker').selectpicker('refresh');
}

S2GridTile.prototype.getQuery = function (object) {
  this.object = object;
  var filters = [];
  if (globalFilters) {
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

  if (object.tileContext.filters) {
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
  if (templateFilters.length > 0) {
    filters = filters.concat(templateFilters);
  }
  var requestTags = {
    "widget": this.object.title,
    "consoleId": getCurrentConsoleId()
  }
  var data = {
    "opcode": "group"
    , "consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    // ,"aggregationField":object.tileContext.aggregationField && object.tileContext.aggregationField != "none" ? object.tileContext.aggregationField : null
    , "aggregationField": object.tileContext.aggregationField
    , "aggregationType": object.tileContext.aggregationType,
    "sourceType": "ECHO_DASHBOARD",
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
    , error: function (xhr, textStatus, error) {
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
    }
  });
}
S2GridTile.prototype.getData = function (data) {
  if (this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }

  var columns = [];
  this.object.tileContext.uiFiltersList = [];
  for (property in data.metricByGrid) {
    var visible = $.inArray(property, this.object.tileContext.uiFiltersSelectedList);
    if ((visible == -1 ? true : false)) {
    columns.push({
      COORDINATES: [parseInt(property.split(",")[1].trim()), parseInt(property.split(",")[0])],
      weight: data.metricByGrid[property]
    });
    }
      this.object.tileContext.uiFiltersList.push(property);
  }
  this.render(data.result, Object.keys({}).length)
}


var S2ColorPalettes = ["#EBFFFF", "#B0F5FF", "#75CFFF", "#489BFF", "#2963EB", "#253AC5", "#26219F", "#301B7B", "#2E1557"];

S2GridTile.prototype.render = function (columns, dataLength) {
  var dataMap = {}
  Object.values = Object.values || function (o) { return Object.keys(o).map(function (k) { return o[k] }) };
  var maxWeight = 1

  var s2Grids = []
  for (var key in columns) {
    s2Grids.push({ token: key, value: columns[key] })
    if (maxWeight < columns[key]) {
      maxWeight = columns[key]
    }
  }
  s2Grids.sort((grid1, grid2) => {
    return grid1.value - grid2.value;
  })


  $("#" + this.object.id).find(".chart-item").find("#" + this.object.id).height('600px')
  $("#" + this.object.id).find(".chart-item").css('margin-top', '35px')
  var layers = []
  layers.push(new deck.S2Layer({
    pickable: true,
    stroked: true,
    filled: true,
    extruded: true,
    wireframe: true,

    elevationScale: 200000,
    data: s2Grids,
    lineWidthScale: 20,
    lineWidthMinPixels: 2,
    getElevation: d => { return d.value / maxWeight; },
    getFillColor: data =>  {return colorScale(S2ColorPalettes, s2Grids.map(d=> d.value), maxWeight, data.value, !this.object.tileContext.quantile) },
    getLineColor: [255, 255, 255],
    getLineWidth: 10,
  }))
  new deck.DeckGL({
    id: this.object.id,
    container: $("#" + this.object.id).find(".chart-item").find("#" + this.object.id)[0],
    mapStyle: 'https://basemaps.cartocdn.com/gl/positron-nolabels-gl-style/style.json',
    initialViewState: {
      longitude: this.object.tileContext.longitude || 77.45,
      latitude: this.object.tileContext.latitude || 12.34, 
      zoom: this.object.tileContext.zoom || 3,
      pitch: 30,
      bearing: 0
    },
    controller: true,
    layers: layers,
    getTooltip: ({ object }) => object && ("GridId: "+object.token + "\nValue: " + object.value),
    onViewStateChange: ({viewState}) => {
      this.object.tileContext.latitude = viewState.latitude;
      this.object.tileContext.longitude = viewState.longitude;
      this.object.tileContext.zoom = viewState.zoom;
    }
  });

}
