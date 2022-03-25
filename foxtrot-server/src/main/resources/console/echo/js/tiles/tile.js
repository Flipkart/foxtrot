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
var wdata1;

function Queue() {
  this.requests = {};
  this.refreshTime = 5;
  this.timeout = 4000;
}
Queue.prototype.enqueue = function (key, ajaxRequest) {
  console.log("Adding: " + key);
  this.requests[key] = ajaxRequest;
};
Queue.prototype.remove = function (key) {
  console.log("Removing: " + key);
  delete this.requests[key];
};
Queue.prototype.start = function () {
  setInterval($.proxy(this.executeCalls, this), this.refreshTime * 1000);
};
Queue.prototype.executeCalls = function () {
  for (var property in this.requests) {
    if (this.requests.hasOwnProperty(property)) {
      this.requests[property]();
    }
  }
};

function Tile() { }

function TileFactory() {
  this.tileObject = "";
}
const FunnelWidget = {
widgetFilter: [],
uiFiltersSelectedList: [],
widgetFilterChange: (filterData)=> {
  FunnelWidget.widgetFilter = filterData;
  console.log("FunnelWidget", this.widgetFilter, filterData, this, FunnelWidget.widgetFilter)
  
},
uiFiltersSelectedListChange: (uiFiltersSelectedData) => {
  FunnelWidget.uiFiltersSelectedList = uiFiltersSelectedData;
  console.log('uiFiltersSelectedList', FunnelWidget.uiFiltersSelectedList)
}
}

// Change period select dropdown values for every tiles
function changeDropdownValue(el) {
  $(el).find(".period-select").val($("#global-filter-period-select").val());
}

function resetPeriodDropdown() { // reset all dropdown values to custom if global filters set to false
  for (var key in tileData) {
    if (tileData.hasOwnProperty(key)) {
      $("#" + key).find(".period-select").val('custom');
    }
  }
}

/**
 * To update period,periodinterval and timeframe in a tiledata
 * it gets updated once user changes time frame in individual widgets
 * @param {*} object 
 */
function changeTimeFrameInformation(object) {
  var selectedValue = $("#" + object.id).find(".period-select").val();
  var separateNumberAndString = seperateStringAndNumber((selectedValue == "custom" ? $("#" + object.id).find(".period-select").text() : selectedValue));
  // index zero is - number, index one is - text
  var period = getPeriodText(separateNumberAndString[1]);// seperate 23h as [23, h]
  var periodInterval = separateNumberAndString[0] + labelPeriodString(period);
  tileData[object.id].tileContext.period = period;
  tileData[object.id].tileContext.periodInterval = periodInterval;
  tileData[object.id].tileContext.timeframe = separateNumberAndString[0];
}


/**
 * 
 * Refresh single tile at at time
 */
function refreshSingleTile(object) {
  isLoggedIn(); // check user is logged in
  var a = new TileFactory();
  a.createGraph(object, $("#" + object.id));
  changeTimeFrameInformation(object);
  if (globalFilters) {
    changeDropdownValue($("#" + key));
  }
}

function refereshTiles() { // auto query for each tile
  isLoggedIn(); // check user is logged in
  for (var key in tileData) {
    if (tileData.hasOwnProperty(key)) {
      var a = new TileFactory();
      a.createGraph(tileData[key], $("#" + key));
      if (globalFilters)
        changeDropdownValue($("#" + key));
    }
  }
}

/** Refresh widgets from choosed value in dropdown */
var refreshInterval;
// get choosed value
function getTimeInterval() {
  var intervalValue = $("#refresh-time").val();
  var multiplyFactor = getRefreshTimeMultiplyeFactor(intervalValue);
  var number = getNumberFromString(intervalValue);
  return number * multiplyFactor;
}

// Start interval
function startRefreshInterval() {
  if ($("#refresh-time").val() == "off") return;
  refreshInterval = setInterval(function () {
    refereshTiles();
    console.log('started');
  }, getTimeInterval());
}

// Stop interval
function stopRefreshInterval() {
  console.log('stopped');
  clearInterval(refreshInterval);
}

// Start and stop
function decideFetchingData() {
  if ($("#refresh-time").val() == "off") {
    stopRefreshInterval();
  } else {
    stopRefreshInterval();
    startRefreshInterval();
  }
}

// Stop and Start intervals 
$("#refresh-time").on('change', function (e) {
  decideFetchingData();
});

setTimeout(startRefreshInterval, 10000); // onLoad start

// when global filters is turned on/off or changed directly refresh tiles
$(".global-filter-period-select").change(function () {
  refereshTiles();
});

/** Refresh widgets from choosed value in dropdown ends */

/**
 * Start and Stop fetching data in n interval 
 * if user goes to another tab stop  fetching data from API
 * If User comes back to tab start fetching data from API
 */
$(window).on("blur focus", function (e) {
  var prevType = $(this).data("prevType");
  if (prevType != e.type) {   //  reduce double fire issues
    switch (e.type) {
      case "blur":
        console.log('Stopped fetching data');
        stopRefreshInterval();
        break;
      case "focus":
        console.log('Started fetching data');
        startRefreshInterval();
        break;
    }
  }
  $(this).data("prevType", e.type);
});

function pushTilesObject(object) { // save each tile data
  tileData[object.id] = object;
  var tabName = (object.tileContext.tabName == undefined ? $(".tab .active").attr('id') : object.tileContext.tabName);
  var tempObject = {
    "id": convertName(tabName),
    "name": tabName,
    "tileList": tileList
    , "tileData": tileData
  }
  if (tileList.length > 0) {
    var deleteIndex = globalData.findIndex(x => x.id == tabName.trim().toLowerCase().split(' ').join("_"));
    if (deleteIndex >= 0) {
      globalData.splice(deleteIndex, 1);
      globalData.splice(deleteIndex, 0, tempObject);
      //tileList = [];
    }
    else {
      globalData.push(tempObject);
    }
  }
}

TileFactory.prototype.updateTileData = function () { // update tile details
  var selectedTile = $("#" + this.tileObject.id);


  // adding changed timeframe units in timeunit dropdown
  var periodSelectElement = selectedTile.find(".period-select");
  $(periodSelectElement).find('option').get(0).remove();
  var timeFrame = this.tileObject.tileContext.timeframe;
  var optionValue = timeFrame + getPeroidSelectString(this.tileObject.tileContext.period);
  var labelString = this.tileObject.tileContext.period;
  var optionLabel = (parseInt(this.tileObject.tileContext.timeframe) <= 1 ? labelString.substring(0, labelString.length - 1) : labelString);
  console.log('===>', timeFrame, optionLabel)
  $(periodSelectElement).prepend('<option selected value="custom">' + timeFrame + '  ' + optionLabel + '</option>');

  selectedTile.find(".tile-title").find(".title-title-span").text(this.tileObject.title);
  selectedTile.find(".tile-title").find(".widget-description").tooltip();
  var widgetDesc = this.tileObject.tileContext.description == undefined ? "Description  N/A" : this.tileObject.tileContext.description;
  selectedTile.find(".tile-title").find(".widget-description").attr("title", widgetDesc);
  var tileid = this.tileObject.id;
  this.createGraph(this.tileObject, selectedTile);
  var tileListIndex = tileList.indexOf(this.tileObject.id);
  var tileDataIndex = tileData[this.tileObject.id];
  delete tileData[tileDataIndex.id];
  tileData[this.tileObject.id] = this.tileObject;
}
TileFactory.prototype.createTileData = function (object) { // store tile list
  var selectedTile = $("#" + object.id);
  selectedTile.find(".tile-title").find(".title-title-span").text(object.title);
  selectedTile.find(".tile-title").find(".widget-description").tooltip();
  var widgetDesc = object.tileContext.description == undefined ? "Description  N/A" : object.tileContext.description;
  selectedTile.find(".tile-title").find(".widget-description").attr("title", widgetDesc);
  var tileid = object.id;
  var prepareTileData = {};
  prepareTileData[object.id] = object;
  pushTilesObject(object);
}
TileFactory.prototype.getTileFormValue = function (form, modal, object) {
  var tileFormValue = {};
  tileFormValue.title = form.find(".tile-title").val();
  tileFormValue.table = form.find(".tile-table").val();
  tileFormValue.id = form.find(".tileId").val();
  tileFormValue.timeUnit = form.find(".tile-time-unit").val();
  tileFormValue.timeValue = form.find(".tile-time-value").val();
  tileFormValue.chartType = form.find(".tile-chart-type").val();
  //updateTile(tileFormValue, modal);
}

function setConfigValue(object) { // set widget form values
  if (currentChartType == "gauge") {
    setGaugeChartFormValues(object);
  }
  else if (currentChartType == "percentageGauge") {
    setPercentageGaugeChartFormValues(object);
  }
  else if (currentChartType == "line") {
    setLineChartFormValues(object);
  }
  else if (currentChartType == "trend") {
    setTrendChartFormValues(object);
  }
  else if (currentChartType == "stacked") {
    setStackedChartFormValues(object);
  }
  else if (currentChartType == "radar") {
    setRadarChartFormValues(object);
  }
  else if (currentChartType == "stackedBar") {
    setStackedBarChartFormValues(object);
  }
  else if (currentChartType == "geo_aggregation") {
    setMapChartFormValues(object);
  }
  else if (currentChartType == "chloropeth") {
    setChloropethFormValues(object);
  }
  else if (currentChartType == "s2grid") {
    setS2GridFormValues(object);
  }
  else if (currentChartType == "pie") {
    setPieChartFormValues(object);
  }
  else if (currentChartType == "statsTrend") {
    setStatsTrendTileChartFormValues(object);
  }
  else if (currentChartType == "bar") {
    setBarChartFormValues(object);
  }
  else if (currentChartType == "count") {
    setCountChartFormValues(object);
  }
  else if (currentChartType == "lineRatio") {
    setLineRatioChartFormValues(object);
  }
  else if (currentChartType == "sunburst") {
    setSunBurstChartFormValues(object);
  }
  else if (currentChartType == "nonStackedLine") {
    setNonStackedLineFormValues(object);
  }
  else if (currentChartType == "funnel") {
    setFunnelChartFormValues(object);
  }
}

function newBtnElement(widget, btnRow) { // create custom btn element
  var columnSize = "";
  var height = "";
  var customClass = "";
  if (widget == "medium") {
    columnSize = "col-md-6 medium-btn-height";
    height = 500;
    customClass = "medium-btn-color";
  } else {
    columnSize = "col-md-3 small-btn-height";
    height = 220;
    customClass = "small-btn-color";
  }
  return "<div class='" + columnSize + " custom-btn-div' style='height:" + height + "px;'><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn filter-nav-button  custom-add-btn " + customClass + "'onClick='setClicketData(this)'  data-toggle='modal' id='row-" + btnRow + "'>+Add widget</button><div>"
}

function move(arr, old_index, new_index) { // move array index
  while (old_index < 0) {
    old_index += arr.length;
  }
  while (new_index < 0) {
    new_index += arr.length;
  }
  if (new_index >= arr.length) {
    var k = new_index - arr.length;
    while ((k--) + 1) {
      arr.push(undefined);
    }
  }
  arr.splice(new_index, 0, arr.splice(old_index, 1)[0]);
  return arr;
}

function renderAfterRearrange() { // move row up and down and refresh object
  clearContainer();
  for (var i = 0; i < tileList.length; i++) {
    renderTiles(tileData[tileList[i]]);
  }
  fetchTableFields();
}

var movedArray = [];

/* move row up */
function upRow(ob) { // row moved up
  movedArray = [];
  var e = $(".tile-container").find(".row-" + ob);
  var prev = ob - 1;
  var previous = $(".tile-container").find(".row-" + prev);
  if (ob != 1) {
    e.prev().insertAfter(e);
    var row = parseInt(ob);

    $(e.find('.tile')).each(function (index) {
      console.log(index + ": " + $(this).attr('id'));
      var tileId = $(this).attr('id');
      var newId = row - 1;
      tileData[tileId].tileContext.row = newId;//change new row number -1
      movedArray.push(tileId);
    });

    $(previous.find('.tile')).each(function (index) {
      console.log(index + ": " + $(this).attr('id'));
      var tileId = $(this).attr('id');
      var newId = row;
      tileData[tileId].tileContext.row = newId;// change new row number +1
      movedArray.push(tileId);
    });
  }

  /* sort array list */
  var keysSorted = sortTiles(tileData);
  tileList = [];
  tileList = keysSorted;
  globalData[getActiveTabIndex()].tileList = keysSorted;
  renderAfterRearrange();
}

function downRow(ob) { // row moved down
  var e = $(".tile-container").find(".row-" + ob);
  e.next().insertBefore(e);
  if (panelRow.length != ob) {
    movedArray = [];
    var e = $(".tile-container").find(".row-" + ob);
    var prev = ob + 1;
    var previous = $(".tile-container").find(".row-" + prev);
    var row = parseInt(ob);

    $(e.find('.tile')).each(function (index) {
      var tileId = $(this).attr('id');
      var newId = row + 1;
      tileData[tileId].tileContext.row = newId; // new row number +1
      movedArray.push(tileId);
    });

    $(previous.find('.tile')).each(function (index) {
      var tileId = $(this).attr('id');
      var newId = row;
      tileData[tileId].tileContext.row = newId; // new row nubmer -1
      movedArray.push(tileId);
    });

    var keysSorted = sortTiles(tileData);
    tileList = [];
    tileList = keysSorted;
    globalData[getActiveTabIndex()].tileList = keysSorted;
    renderAfterRearrange();
  }
}

// create new div
TileFactory.prototype.createNewRow = function (tileElement) {
  tileElement.addClass("col-md-12"); // add class for div which is full width
  tileElement.addClass("max-height");
  if (panelRow.length == 0) { // initial page
    row = 1;
    panelRow.push({
      widgetType: this.tileObject.tileContext.widgetType
      , id: this.tileObject.id
    });
    tileElement.addClass("row-" + row);
  }
  else { // incremetn row value by one
    panelRow.push({
      widgetType: this.tileObject.tileContext.widgetType
      , id: this.tileObject.id
    });
    row = panelRow.length;
    tileElement.addClass("row-" + row);
  }
  tileElement.prepend('<div id="arrow-btn"><button type="button"onClick="upRow(' + row + ')" class="row-identifier-' + row + ' up-arrow arrow-up" id="row-up"><img class="arrow-up" src="img/context-arrow-up-hover.png" /></button><button type="button" onClick="downRow(' + row + ')" class="row-identifier-' + row + '" id="row-down"><img class="down" src="img/context-arrow-down-hover.png"/></button></div>');

  if (this.tileObject.tileContext.widgetType != "full") { // dont add row add button for full widget
    var btnRow = row;
    var newBtn = newBtnElement(this.tileObject.tileContext.widgetType, btnRow);
    tileElement.append(newBtn);
  }
  return tileElement;
}
TileFactory.prototype.updateFilterCreation = function (object) { // setting widget form values
  currentChartType = object.tileContext.chartType;
  //removeFilters();
  var tileListIndex = tileList.indexOf(object.id);
  var tileDataIndex = tileData[tileListIndex];
  var selectedTileObject = tileData[object.id];

  var form = $("#sidebar").find("form");
  if (object.tileContext.tableDropdownIndex == undefined) {
    var tableDropdownArray= new Array();
    var tableDropdown = document.getElementById('tileTable');
    for (i = 0; i < tableDropdown.options.length; i++) {
    tableDropdownArray[i] = tableDropdown .options[i].text;
  }
  tableDropdownArray.reverse();

  form.find(".tile-table").val(parseInt(tableDropdownArray.indexOf(object.tileContext.table)));
  }
  else {
    form.find(".tile-table").val(parseInt(object.tileContext.tableDropdownIndex));
  }
  $('.tile-table').selectpicker('refresh');


  if (selectedTileObject) {
    setConfigValue(selectedTileObject);
  }

  if (object.tileContext.filters.length > 0) {
    filterRowArray = [];
    for (var invokeFilter = 0; invokeFilter < selectedTileObject.tileContext.filters.length; invokeFilter++) {
      addFilters();
    }
    //setFilters(object);
    setTimeout(function () { //calls click event after a certain time
      setFilters(selectedTileObject.tileContext.filters);
    }, 1000);
  }

}
TileFactory.prototype.updateFilters = function (filters) {
  var instanceVar = this;
  var temp = [];
  instanceVar.tileObject.tileContext.uiFiltersSelectedList = arr_diff(instanceVar.tileObject.tileContext.uiFiltersList, filters);
  if(instanceVar.tileObject.tileContext.chartType === "funnel") {
    FunnelWidget.uiFiltersSelectedListChange(instanceVar.tileObject.tileContext.uiFiltersSelectedList);
  }
}
// Filter configuration
TileFactory.prototype.triggerFilter = function (tileElement, object) { // filter modal
  if (object.tileContext.chartType != "radar" && object.tileContext.chartType != "line" && object.tileContext.chartType != "lineRatio" && object.tileContext.chartType != "sunburst" ) {
    var instanceVar = this;
    tileElement.find(".widget-toolbox").find(".filter").click(function () {
      clearFilterValues();
      if( object.tileContext.chartType === "funnel") {
        object.tileContext.uiFiltersList = [...FunnelWidget.widgetFilter];
      }
      var modal = $("#setupFiltersModal").modal('show');
      console.log(' FunnelWidget.widgetFilter',  FunnelWidget.widgetFilter)
      var fv = $("#setupFiltersModal").find(".filter_values");
      var form = modal.find("form");
      form.off('submit');
      form.on('submit', $.proxy(function (e) {
        instanceVar.updateFilters(getFilterCheckBox());
        $("#setupFiltersModal").modal('hide');
        e.preventDefault();
      }));
      var options = [];
      if (object.tileContext.uiFiltersList == undefined) return;
      
      for (var i = 0; i < object.tileContext.uiFiltersList.length; i++) {
        var value = object.tileContext.uiFiltersList[i];
        var index = $.inArray(value, object.tileContext.uiFiltersSelectedList);
        if (index == -1) {
          $("#filter-checkbox-div").append('<div class="ui-filter-list"><label><input name="filter-checkbox" class="ui-filter-checkbox" type="checkbox" value="' + value + '" checked="checked" onclick="listenUiFilterCheck();"><span>' + value + '</span></label>  </div>');
        } else {
          $("#filter-checkbox-div").append('<div class="ui-filter-list"><label><input name="filter-checkbox" onclick="listenUiFilterCheck();" class="ui-filter-checkbox" type="checkbox" value="' + value + '"><span>' + value + '</span></label>  </div>');
        }
        if (object.tileContext.uiFiltersSelectedList) {
          if (object.tileContext.uiFiltersSelectedList.length > 0) {
            showUnselectAllAction();
          } else {
            showSelectAllAction();
          }
        }
      }
    });
  }
}


//  -------------------- Starts Added download widget 2--------------------


TileFactory.prototype.downloadWidget = function (object, tileElement) { // get query
  if (object.tileContext.chartType == "line") {
    var lineGraph = new LineTile();
    //lineGraph.render(tileElement, object);
    lineGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "radar") {
    tileElement.find(".chart-item").append('<div id="radar-' + object.id + '" style="width:200;height:200"></div>');
    var radarGraph = new RadarTile();
    radarGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "funnel") {
    tileElement.find(".chart-item").append('<div id="funnel-' + object.id + '" style="width:200;height:200"></div>');
    var funnelGraph = new FunnelTile();
    funnelGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "trend") {
    var trendGraph = new TrendTile();
    trendGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "gauge") {
    var gaugeGraph = new GaugeTile();
    gaugeGraph.downloadWidget(object);
  } else if (object.tileContext.chartType == "percentageGauge") {
    var gaugeGraph = new PercentageGaugeTile();
    gaugeGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "stacked") {
    var stackedGraph = new StackedTile();
    stackedGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "stackedBar") {
    var stackedBarGraph = new StackedBarTile();
    stackedBarGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "pie") {
    var pieGraph = new PieTile();
    pieGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "statsTrend") {
    var statsTrendGraph = new StatsTrendTile();
    statsTrendGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "bar") {
    var barGraph = new BarTile();
    barGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "count") {
    var countGraph = new CountTile();
    countGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "lineRatio") {
    var lineRatioGraph = new LineRatioTile();
    lineRatioGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "sunburst") {
    var sunburstGraph = new SunburstTile();
    sunburstGraph.downloadWidget(object);
  }
  else if (object.tileContext.chartType == "nonStackedLine") {
    var nonStackedLineGraph = new NonStackedLineTile();
    nonStackedLineGraph.downloadWidget(object);
  }
}



TileFactory.prototype.triggerDownload = function (tileElement, object) {
  var instanceVar = this;
  tileElement.find(".download-widget").click(function () {
    var clickedObject = tileData[object.id];
    console.log("object clickedObject......", clickedObject);
    instanceVar.downloadWidget(object,tileElement)
  });
}



//  common Recussive function for download csv using blob-------
function downloadTextAsCSV(text, fileName) {
  var link = document.createElement('a');
  link.href = 'data:text/csv;charset=utf-8,' + encodeURI(text);
  link.target = "_blank";
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}


//  -------------------- Ends added download widget 2--------------------

/**
 * 
 * add change event to period select dropdown
 */
TileFactory.prototype.addEventToPeriodSelect = function (tileElement, object) {
  tileElement.find(".period-select").change(function () {
    refreshSingleTile(tileData[object.id]);// refresh immediately
  });
};


// Add click event for tile config icon
TileFactory.prototype.triggerConfig = function (tileElement, object) { // code to show sidebar when edit
  var instanceVar = this;
  tileElement.find(".widget-toolbox").find(".glyphicon-cog").click(function () {
    $(".copy-widget-btn").show();
    object = tileData[object.id];
    isEdit = true;
    editingRow = object.tileContext.row;
    showHideSideBar();
    $('.tile-container').find("#" + object.id).addClass('highlight-tile');
    //$("#addWidgetModal").modal('show');
    $("#sidebar").find(".tileId").val(object.id);
    $(".chart-type").attr('disabled', true);

    var form = $("#sidebar").find("form");
    form.find(".tile-title").val(object.title);
    var tileDescription = object.tileContext.description == undefined ? "" : object.tileContext.description;
    form.find("#tile-description").val(tileDescription);
    form.find("#sidebar-tileId").val(object.id);

    $(".chart-type").val(object.tileContext.chartType)

    if (object.tileContext.tableFields != undefined) { // this is for without console
      currentFieldList = object.tileContext.tableFields;
    }
    else { // with console
      currentFieldList = tableFiledsArray[object.tileContext.table].mappings;
    }

    clickedChartType($(".chart-type"));

    setTimeout(function () { instanceVar.updateFilterCreation(object); }, 1000);
    $(".delete-widget").show();
    $("#delete-widget-divider").show();
    $(".save-widget-btn").show();
    $("#delete-widget-value").val(object.id);
    $("#copy-widget-value").data("tile", object);
  });
}
TileFactory.prototype.triggerChildBtn = function (tileElement, object) { // child btn
  var instanceVar = this;
  tileElement.find(".add-child-btn").find(".child-btn").click(function () {
    $("#addWidgetModal").modal('show');
    $("#addWidgetModal").find(".child-tile").val('true');
    $(".vizualization-type").hide();
    instanceVar.updateFilterCreation(object);
  });
}
// Save action for tile config save button
TileFactory.prototype.saveTileConfig = function (object) { // save tile
  $("#tile-configuration").find(".save-changes").click(function () {
    var form = $("#tile-configuration").find("form");
    form.off('submit');
    form.on('submit', $.proxy(function (e) {
      this.getTileFormValue(form, "tile-configuration", object)
      $("#tile-configuration").modal('hide');
      e.preventDefault();
    }, object));
  });
}
TileFactory.prototype.createGraph = function (object, tileElement) { // get query
  if (object.tileContext.chartType == "line") {
    var lineGraph = new LineTile();
    //lineGraph.render(tileElement, object);
    lineGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "radar") {
    tileElement.find(".chart-item").append('<div id="radar-' + object.id + '" style="width:200;height:200"></div>');
    var radarGraph = new RadarTile();
    radarGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "trend") {
    var trendGraph = new TrendTile();
    trendGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "gauge") {
    var gaugeGraph = new GaugeTile();
    gaugeGraph.getQuery(object);
  } else if (object.tileContext.chartType == "percentageGauge") {
    var gaugeGraph = new PercentageGaugeTile();
    gaugeGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "stacked") {
    var stackedGraph = new StackedTile();
    stackedGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "stackedBar") {
    var stackedBarGraph = new StackedBarTile();
    stackedBarGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "geo_aggregation") {
    var pieGraph = new MapTile();
    pieGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "chloropeth") {
    var pieGraph = new ChloropethTile();
    pieGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "s2grid") {
    var pieGraph = new S2GridTile();
    pieGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "pie") {
    var pieGraph = new PieTile();
    pieGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "statsTrend") {
    var statsTrendGraph = new StatsTrendTile();
    statsTrendGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "bar") {
    var barGraph = new BarTile();
    barGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "count") {
    var countGraph = new CountTile();
    countGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "lineRatio") {
    var lineRatioGraph = new LineRatioTile();
    lineRatioGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "sunburst") {
    var sunburstGraph = new SunburstTile();
    sunburstGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "nonStackedLine") {
    var nonStackedLineGraph = new NonStackedLineTile();
    nonStackedLineGraph.getQuery(object);
  }
  else if (object.tileContext.chartType == "funnel") {
    tileElement.find(".chart-item").append('<div id="funnel-' + object.id + '" style="width:200;height:200"></div>');
    var funnelGraph = new FunnelTile();
    object.tileContext.uiFiltersList = [...FunnelWidget.widgetFilter];
    object.tileContext.uiFiltersSelectedList = FunnelWidget.uiFiltersSelectedList.length === 0 ? undefined : [...FunnelWidget.uiFiltersSelectedList];
    funnelGraph.getQuery(object);
  }
}
TileFactory.prototype.create = function () {
  var tileElement = $(handlebars("#tile-template", {
    tileId: this.tileObject.id
    , title: this.tileObject.title
  }));

  if (this.tileObject.tileContext.isnewRow) {
    isNewRowCount = 0;
    tileColumn = 1;
    firstWidgetType = this.tileObject.tileContext.widgetType;
  } else {
    isNewRowCount++;
    var column = $(".tile-container").find(".row-" + this.tileObject.tileContext.row).find(".tile").length;
    tileColumn = column + 1;
  }

  this.tileObject.tileContext.position = tileColumn;

  var smallWidgetCountForRow = $('.row-' + this.tileObject.tileContext.row).find(".small-widget").length;
  var MediumWidgetCountForRow = $('.row-' + this.tileObject.tileContext.row).find(".medium-widget").length;
  if (MediumWidgetCountForRow == 1) {
    tileElement.find(".tile").addClass((this.tileObject.tileContext.isnewRow ? 'full-widget-max-width' : 'full-widget-min-width'));
  } else if ((smallWidgetCountForRow == 1) & (this.tileObject.tileContext.widgetType == "full")) {
    tileElement.find(".tile").addClass('full-widget-medium-width');
    this.tileObject.tileContext.widgetSize = 9;
  }
  else if ((smallWidgetCountForRow == 2) & (this.tileObject.tileContext.widgetType == "full")) {
    tileElement.find(".tile").addClass('full-widget-min-width');
    this.tileObject.tileContext.widgetSize = 6;
  }
  else if ((smallWidgetCountForRow == 3) & (this.tileObject.tileContext.widgetType == "full")) {
    tileElement.find(".tile").addClass('full-widget-small-width');
    this.tileObject.tileContext.widgetSize = 3;
  }
  else if (this.tileObject.tileContext.widgetType == "full") {
    this.tileObject.tileContext.isnewRow = true;
    this.tileObject.tileContext.widgetSize = 12;
  }

  var clickedRow; // clicked row
  if (this.tileObject.tileContext.isnewRow) {
    tileElement = this.createNewRow(tileElement);
    row = this.tileObject.tileContext.row;
  } else {
    row = this.tileObject.tileContext.row;
    if (isNewConsole) {
      tileElement.append(newBtnElement(this.tileObject.tileContext.widgetType, row));
    }
  }

  if (this.tileObject.tileContext.widgetType == "medium") {
    tileElement.find(".tile").addClass('col-sm-6 medium-widget');
    tileElement.find(".tile").height(460);
    tileElement.find(".widget-header").css("background-color", "#fff");
  }
  else if (this.tileObject.tileContext.widgetType == "small") {
    tileElement.find(".tile").addClass('col-sm-3 small-widget');
    tileElement.find(".tile").height(220);
    tileElement.find(".widget-header").css("background-color", "#fff");
    tileElement.find(".widget-header").height(68);
    tileElement.find(".widget-header > .tile-title").css("font-size", "12px");
    tileElement.find(".widget-header > .tile-title").css("width", "112px");
    tileElement.find(".widget-header > .tile-title").addClass("small-widget-title");
  }

  if (this.tileObject.tileContext.chartType == "radar") {
    tileElement.find(".trend-chart").remove();
    tileElement.find(".chart-item").addClass("radar-chart");
  }
  else if (this.tileObject.tileContext.chartType == "line" || this.tileObject.tileContext.chartType == "geo_aggregation" || this.tileObject.tileContext.chartType == "chloropeth"|| this.tileObject.tileContext.chartType == "s2grid"|| this.tileObject.tileContext.chartType == "stacked" || this.tileObject.tileContext.chartType == "stackedBar" || this.tileObject.tileContext.chartType == "pie" || this.tileObject.tileContext.chartType == "statsTrend" || this.tileObject.tileContext.chartType == "bar" || this.tileObject.tileContext.chartType == "lineRatio" || this.tileObject.tileContext.chartType == "nonStackedLine") {
    /*tileElement.find(".widget-header").append('<div id="' + this.tileObject.id + '-health-text" class="lineGraph-health-text">No Data available</div>');*/
    tileElement.find(".widget-header").append('<div id="' + this.tileObject.id + '-health" style=""></div>');
    tileElement.find(".chart-item").append('<div class="row"><div id="' + this.tileObject.id + '"></div><div class="legend"></div></div>');
    //tileElement.find(".chart-item").append("")
  }
  if (this.tileObject.tileContext.isnewRow) { // new row
    tileElement.insertBefore('.float-clear');
  }
  else { // remove row btn and add new div based on type
    $(tileElement).insertBefore($('.row-' + row).find(".custom-btn-div"));
    $('.row-' + row).find(".custom-btn-div").remove();
    $('.row-' + row).append(newBtnElement(this.tileObject.tileContext.widgetType, this.tileObject.tileContext.row));
  }

  if (this.tileObject.tileContext.widgetType == "small") {
    tileElement.find(".settings").addClass('reduce-filter-size');
    tileElement.find(".widget-timeframe").addClass('reduce-filter-option');
    tileElement.find(".period-select").addClass('reduce-period-select');
    tileElement.find(".settings-icon").addClass('reduce-settings-icon');
    tileElement.find(".filter").hide();
  }
  else if (this.tileObject.tileContext.widgetType == "medium") {
    tileElement.find(".widget-header").addClass('reduce-widget-header-size');
  }

  if ($('.row-' + row).find(".small-widget").length == 4) {
    $('.row-' + row).find(".custom-btn-div").remove();
  }

  var periodSelectElement = tileElement.find(".period-select");

  var timeFrame = this.tileObject.tileContext.timeframe;
  var optionValue = timeFrame + getPeroidSelectString(this.tileObject.tileContext.period);
  var labelString = this.tileObject.tileContext.period;
  if (labelString) { // check its not null
    var optionLabel = (parseInt(this.tileObject.tileContext.timeframe) <= 1 ? labelString.substring(0, labelString.length - 1) : labelString);
    $(periodSelectElement).prepend('<option selected value="custom">' + timeFrame + '  ' + optionLabel + '</option>');
  }


  this.createGraph(this.tileObject, tileElement);
  this.triggerConfig(tileElement, this.tileObject); // add event for tile config
  this.triggerFilter(tileElement, this.tileObject);
  this.triggerDownload(tileElement, this.tileObject);  // adding download btn
  //this.triggerChildBtn(tileElement,this.tileObject);
  this.createTileData(this.tileObject);
  this.saveTileConfig(this.tileObject); // add event for tile save btn

  previousWidget = this.tileObject.tileContext.widgetType;
  this.addEventToPeriodSelect(tileElement, this.tileObject);
};






