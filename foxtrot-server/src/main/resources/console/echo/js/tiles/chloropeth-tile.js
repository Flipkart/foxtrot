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
function ChloropethTile() {
  this.object = "";
}

function getChloropethFormValues() {
  var period = $("#chloropeth-time-unit").val();
  var timeframe = $("#chloropeth-timeframe").val();
  var latitude = parseFloat($("#chloropeth-latitude").val());
  var longitude = parseFloat($("#chloropeth-longitude").val());
  var zoom = parseFloat($("#chloropeth-zoom").val());
  var eventField = $("#chloropeth-event-field").val();
  var uniqueKey = $("#chloropeth-uniquekey").val();
  var ignoreDigits = $(".chloropeth-ignored-digits").val();
  var selectedValue = $("#chloropeth-selected-value").val();
  var sortingbar =$('.chloropeth-sorting-digits').is(':checked');
  var aggregationType = $('#chloropeth-aggregation-type').val();
  var geoLayer = $('#chloropeth-geo-layer').val();
  var aggregationField = $('#chloropeth-aggregation-field').val();

  
 
  if (eventField == "none") {
    return [[], false];
  }
  var groupingString = currentFieldList[parseInt(eventField)].field;
  var status = true;

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

  return {
    "period": period
    , "timeframe": timeframe
    , "nesting": [groupingString]
    , "uniqueKey": uniqueKey
    , "ignoreDigits" : ignoreDigits
    , "selectedValue": selectedValue
    , "sortingbar":sortingbar
    , "aggregationType":aggregationType
    , "aggregationField":aggregationField
    , "geoLayer":geoLayer,
    "latitude": latitude,
    "longitude": longitude,
    "zoom": zoom
  };
}

function setChloropethFormValues(object) {
  var geoLayers = ['state', 'city'];
  var agg_types =
  $("#chloropeth-time-unit").val(object.tileContext.period);
  $("#chloropeth-time-unit").selectpicker('refresh');
  $("#chloropeth-timeframe").val(object.tileContext.timeframe);
  $("#chloropeth-latitude").val(object.tileContext.latitude);
  $("#chloropeth-longitude").val(object.tileContext.longitude);
  $("#chloropeth-zoom").val(object.tileContext.zoom);
  $("#chloropeth-event-field").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.nesting[0])));
  $("#chloropeth-event-field").selectpicker('refresh');
  $("#chloropeth-uniquekey").val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueKey)));
  $("#chloropeth-uniquekey").selectpicker('refresh');
  $(".chloropeth-ignored-digits").val(parseInt(object.tileContext.ignoreDigits == undefined ? 0 : object.tileContext.ignoreDigits));
  $("#chloropeth-selected-value").val((object.tileContext.selectedValue == undefined ? '' : object.tileContext.selectedValue));
  $('#chloropeth-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#chloropeth-aggregation-field').selectpicker('refresh');
  $("#chloropeth-geo-layer").selectpicker('val', object.tileContext.geoLayer);
  $('#chloropeth-aggregation-type').selectpicker('val', object.tileContext.aggregationType);

  // ------  start for checkbox set values -------
  if(object.tileContext.sortingbar===true){
    $("#chloropeth-sorting-digits").prop("checked", true);
  }
  else{
    $("#chloropeth-sorting-digits").val((object.tileContext.sortingbar == undefined ? 'undefined' : object.tileContext.sortingbar));
  }
}

function clearChloropethForm() {
  $('.chloropethForm')[0].reset();
  $(".chloropethForm").find('.selectpicker').selectpicker('refresh');
}

ChloropethTile.prototype.getQuery = function (object) {
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
    "opcode": "group"
    ,"consoleId": getCurrentConsoleId()
    , "table": object.tileContext.table
    , "filters": filters
    , "uniqueCountOn": object.tileContext.uniqueKey && object.tileContext.uniqueKey != "none" ? object.tileContext.uniqueKey : null
    , "nesting": object.tileContext.nesting
    // ,"aggregationField":object.tileContext.aggregationField && object.tileContext.aggregationField != "none" ? object.tileContext.aggregationField : null
     ,"aggregationField":object.tileContext.aggregationField
    ,"aggregationType":object.tileContext.aggregationType,
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
ChloropethTile.prototype.getData = function (data) {
  if(this.object.tileContext.uiFiltersList == undefined) {
    this.object.tileContext.uiFiltersList = [];
    this.object.tileContext.uiFiltersSelectedList = [];
  }

  var columns = [];
  // this.object.tileContext.uiFiltersList = [];
  for (property in data.metricByGrid) {
    var visible = $.inArray( property, this.object.tileContext.uiFiltersSelectedList);
    // if ((visible == -1 ? true : false)) {
      columns.push({
        COORDINATES: [parseInt(property.split(",")[1].trim()), parseInt(property.split(",")[0])],
        weight: data.metricByGrid[property]
      });
    // }
  //   this.object.tileContext.uiFiltersList.push(property);
  }
  $.ajax({
    method: "post"
    , dataType: 'json'
    , accepts: {
      json: 'application/json'
    }
    , url: apiUrl + "/v1/geojson/"+this.object.tileContext.geoLayer
    , contentType: "application/json"
    , data: JSON.stringify({})
    , success:(dataRegions) => {
      this.render(data.result, Object.keys({}).length, dataRegions);
    }
    , error:(xhr, textStatus, error )=> {
      showFetchError(refObject, "refresh", JSON.parse(xhr.responseText))
    }
  });

}
var ChloropethColorPalettes = ["#EBFFFF", "#B0F5FF", "#75CFFF", "#489BFF", "#2963EB", "#253AC5", "#26219F", "#301B7B", "#2E1557"];

ChloropethTile.prototype.render = function (columns, dataLength, regionDefs) {
  var dataMap = {}
  Object.values = Object.values || function(o){return Object.keys(o).map(function(k){return o[k]})};

  var maxWeight = 1
  for(var key in columns) {
    dataMap[key.toLowerCase()] = {'weight': columns[key]}
    if(columns[key] > maxWeight) {
      maxWeight = columns[key]
    }
  }

  regionDefs.forEach(region => {
    if(dataMap[region.id.toLowerCase()] !== undefined) {
      dataMap[region.id.toLowerCase()] = Object.assign({}, region, dataMap[region.id.toLowerCase()]);
    }
  });

  $("#"+this.object.id).find(".chart-item").find("#"+this.object.id).height('600px')
  $("#"+this.object.id).find(".chart-item").css('margin-top', '35px')
  var layers = []
  Object.values(dataMap).filter(d => d.properties !== undefined).forEach(data => layers.push(new deck.GeoJsonLayer({
    pickable: true,
    stroked: true,
    filled: true,
    extruded: true,
    wireframe: true,
    elevationScale: 200000,
    data:  {"type": "Feature","geometry": data.geometry, "properties":{regionName: data.properties.name, weight: data.weight || 0}},
    lineWidthScale: 20,
    lineWidthMinPixels: 2,
    getFillColor: data =>colorScaleQuantize(ChloropethColorPalettes, maxWeight, data.properties.weight|| 0),
    getLineColor: [255, 255, 255],
    getLineWidth: 10,
    getElevation: d => { return d.properties.weight/maxWeight;}
  })))
  new deck.DeckGL({
    id: this.object.id,
    container: $("#"+this.object.id).find(".chart-item").find("#"+this.object.id)[0],
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
    getTooltip: ({object}) => object && ("Region: "+object.properties.regionName +"\nValue: "+ object.properties.weight  ),
    onViewStateChange: ({viewState}) => {
      this.object.tileContext.latitude = viewState.latitude;
      this.object.tileContext.longitude = viewState.longitude;
      this.object.tileContext.zoom = viewState.zoom;
    }
  });
  }
