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

function Tile() {}

function TileFactory() {
  this.tileObject = "";
}

function pushTilesObject(object) {
  tileData[object.id] = object;
  interval = setInterval(function () {
    var a = new TileFactory();
    a.createGraph(object, $("#"+ object.id));
  }, 6000);
}
TileFactory.prototype.updateTileData = function () {
  var selectedTile = $("#" + this.tileObject.id);
  selectedTile.find(".tile-title").text(this.tileObject.title);
  var tileid = this.tileObject.id;
  this.createGraph(this.tileObject, selectedTile);
  var tileListIndex = tileList.indexOf(this.tileObject.id);
  var tileDataIndex = tileData[this.tileObject.id];
  delete tileData[tileDataIndex.id];
  tileData[this.tileObject.id] = this.tileObject;
}
TileFactory.prototype.createTileData = function (object) {
  var selectedTile = $("#" + object.id);
  selectedTile.find(".tile-title").text(object.title);
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

function setConfigValue(object) {
  var form = $("#addWidgetModal").find("form");
  form.find(".tile-title").val(object.title);
  if (object.tileContext.tableDropdownIndex == undefined) {
    form.find(".tile-table").val(parseInt(tableNameList.indexOf(object.tileContext.table)));
  }
  else {
    form.find(".tile-table").val(parseInt(object.tileContext.tableDropdownIndex));
  }
  /*
    form.find(".tile-time-unit").val(object.tileContext.timeUnit);
    form.find(".tile-time-value").val(object.tileContext.timeValue);
    form.find(".tile-chart-type").val(object.tileContext.chartType);
  */
  $('.tile-table').selectpicker('refresh');
  var chartElement = $("#vizualization").find("[data-chart-type='" + object.tileContext.chartType + "']");
  clickedChartType(chartElement);
  if (currentChartType == "gauge") {
    setGaugeChartFormValues(object);
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
  else if (currentChartType == "pie") {
    setPieChartFormValues(object);
  }
  else if (currentChartType == "statsTrend") {
    setStatsTrendTileChartFormValues(object);
  }
  else if (currentChartType == "bar") {
    setBarChartFormValues(object);
  }
}

function clearForm() {
  var form = $("#tile-configuration").find("form");
  form.find(".tile-title").val('');
  form.find(".tile-table").val('');
  form.find(".tile-time-unit").val('');
  form.find(".tile-time-value").val('');
  form.find(".tile-chart-type").val('');
}

function newBtnElement() {
  return "<div class='col-md-2 custom-btn-div'><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn btn btn-primary filter-nav-button glyphicon glyphicon-plus custom-add-btn'onClick='setClicketData(this)'  data-toggle='modal' id='row-" + row + "'></button><div>"
}
// create new div
TileFactory.prototype.createNewRow = function (tileElement) {
  tileElement.addClass("col-md-12"); // add class for div which is full width
  if (panelRow.length == 0) { // initial page
    row = 1;
    panelRow.push({
      widgetType: this.tileObject.widgetType
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
  if (this.tileObject.tileContext.widgetType != "full") // dont add row add button for full widget
    tileElement.append(newBtnElement());
  return tileElement;
}
TileFactory.prototype.updateFilterCreation = function (object) {
  currentChartType = object.tileContext.chartType;
  removeFilters();
  var tileListIndex = tileList.indexOf(object.id);
  var tileDataIndex = tileData[tileListIndex];
  var selectedTileObject = tileData[object.id];
  if ($("#listConsole").val() == "none") { // this is for without console
    currentFieldList = object.tileContext.tableFields;
  }
  else { // with console
    currentFieldList = tableFiledsArray[object.tileContext.table].mappings;
  }
  if (object.tileContext.filters.length > 0) {
    filterRowArray = [];
    for (var invokeFilter = 0; invokeFilter < selectedTileObject.tileContext.filters.length; invokeFilter++) {
      addFitlers();
    }
    //setFilters(object);
    setTimeout(function () { //calls click event after a certain time
      setFilters(selectedTileObject.tileContext.filters);
    }, 1000);
  }
  if (selectedTileObject) {
    setConfigValue(selectedTileObject);
  }
}
TileFactory.prototype.updateFilters = function (filters) {
  var instanceVar = this;
  var temp = [];
  instanceVar.tileObject.tileContext.uiFiltersSelectedList = arr_diff(instanceVar.tileObject.tileContext.uiFiltersList, filters)
}
  // Filter configuration
TileFactory.prototype.triggerFilter = function (tileElement, object) {
    var instanceVar = this;
    tileElement.find(".widget-toolbox").find(".filter").click(function () {
      clearFilterValues();
      var modal = $("#setupFiltersModal").modal('show');
      var fv = $("#setupFiltersModal").find(".filter_values");
      fv.multiselect('refresh');
      var form = modal.find("form");
      form.off('submit');
      form.on('submit', $.proxy(function (e) {
        instanceVar.updateFilters($("#filter_values").val());
        $("#setupFiltersModal").modal('hide');
        e.preventDefault();
      }));
      var options = [];
      if (object.tileContext.uiFiltersList == undefined) return;
      for (var i = 0; i < object.tileContext.uiFiltersList.length; i++) {
        var value = object.tileContext.uiFiltersList[i];
        var index = $.inArray( value, object.tileContext.uiFiltersSelectedList)
        options.push({
          label: value
          , title: value
          , value: value
          , selected: (index == -1 ? true : false)
        });
      }
      fv.multiselect('dataprovider', options);
      fv.multiselect('refresh');
    });
  }
  // Add click event for tile config icon
TileFactory.prototype.triggerConfig = function (tileElement, object) {
  var instanceVar = this;
  tileElement.find(".widget-toolbox").find(".glyphicon-cog").click(function () {
    $("#addWidgetModal").modal('show');
    $("#addWidgetModal").find(".tileId").val(object.id);
    $(".vizualization-type").hide();
    instanceVar.updateFilterCreation(object);
  });
}
TileFactory.prototype.triggerChildBtn = function (tileElement, object) {
    var instanceVar = this;
    tileElement.find(".add-child-btn").find(".child-btn").click(function () {
      $("#addWidgetModal").modal('show');
      $("#addWidgetModal").find(".child-tile").val('true');
      $(".vizualization-type").hide();
      instanceVar.updateFilterCreation(object);
    });
  }
  // Save action for tile config save button
TileFactory.prototype.saveTileConfig = function (object) {
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
TileFactory.prototype.createGraph = function (object, tileElement) {
  if (object.tileContext.chartType == "line") {
    var lineGraph = new LineTile();
    //lineGraph.render(tileElement, object);
    lineGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "radar") {
    tileElement.find(".chart-item").append('<div id="radar-' + object.id + '" style="width:200;height:200"></div>');
    var radarGraph = new RadarTile();
    radarGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "trend") {
    var trendGraph = new TrendTile();
    trendGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "gauge") {
    var gaugeGraph = new GaugeTile();
    gaugeGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "stacked") {
    var stackedGraph = new StackedTile();
    stackedGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "stackedBar") {
    var stackedBarGraph = new StackedBarTile();
    stackedBarGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "pie") {
    var pieGraph = new PieTile();
    pieGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "statsTrend") {
    var statsTrendGraph = new StatsTrendTile();
    statsTrendGraph.getQuery(tileElement, object);
  }
  else if (object.tileContext.chartType == "bar") {
    var barGraph = new BarTile();
    barGraph.getQuery(tileElement, object);
  }
}
TileFactory.prototype.create = function () {
  var tileElement = $(handlebars("#tile-template", {
    tileId: this.tileObject.id
    , title: this.tileObject.title
  }));
  var row = 0; // row
  var clickedRow; // clicked row
  if (defaultPlusBtn) { // check its new row
    tileElement = this.createNewRow(tileElement)
  }
  else { // row button action
    var splitValue = customBtn.id.split("-");
    var rowObject = panelRow[splitValue[1] - 1];
    clickedRow = rowObject.id
    if (this.tileObject.tileContext.widgetType != rowObject.widgetType) { // f choosen type and row type is not equal
      tileElement = this.createNewRow(tileElement);
      defaultPlusBtn = true;
    }
    if (this.tileObject.tileContext.widgetType == 'small' && rowObject.widgetType == 'small') {
      var findElement = $("." + customBtn.id);
      var column1Length = findElement.find(".row-col-1").length;
      if (column1Length == 0 || column1Length == 2) {
        if (column1Length == 0) {
          tileElement.addClass('row-col-1 col-md-3');
        }
        else if (column1Length == 2) {
          tileElement.addClass('row-col-2 col-md-3');
        }
        var rowCol2Length = findElement.find(".row-col-2").length;
        if (rowCol2Length == 0) tileElement.append("<div class='widget-add-btn'><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn btn btn-primary filter-nav-button glyphicon glyphicon-plus custom-add-btn row-col-1'onClick='setClicketData(this)'  data-toggle='modal' id='row-" + splitValue[1] + "'></button><div>");
      }
    }
  }
  if (this.tileObject.tileContext.widgetType == "full") {
    tileElement.find(".tile").addClass('col-md-12');
  }
  else if (this.tileObject.tileContext.widgetType == "medium") {
    tileElement.find(".tile").addClass('col-md-6');
    tileElement.find(".tile").width(590);
    tileElement.find(".tile").height(460);
    tileElement.find(".widget-header").css("background-color", "#fff");
  }
  else if (this.tileObject.tileContext.widgetType == "small") {
    tileElement.find(".tile").addClass('col-md-3');
    tileElement.find(".tile").width(280);
    tileElement.find(".tile").height(250);
    tileElement.find(".widget-header").css("background-color", "#fff");
    tileElement.find(".widget-header").height(68);
  }
  if (this.tileObject.tileContext.chartType == "radar") {
    tileElement.find(".trend-chart").remove();
    tileElement.find(".chart-item").addClass("radar-chart");
  }
  else if (this.tileObject.tileContext.chartType == "line" || this.tileObject.tileContext.chartType == "stacked" || this.tileObject.tileContext.chartType == "stackedBar" || this.tileObject.tileContext.chartType == "pie" || this.tileObject.tileContext.chartType == "statsTrend" || this.tileObject.tileContext.chartType == "bar") {
    tileElement.find(".widget-header").append('<div id="' + this.tileObject.id + '-health-text" class="lineGraph-health-text">No Data available</div>');
    tileElement.find(".widget-header").append('<div id="' + this.tileObject.id + '-health" style=""></div>');
    tileElement.find(".chart-item").append('<div id="' + this.tileObject.id + '"></div>');
    tileElement.find(".chart-item").append("<div style='height:100px;' class='legend col-md-12'></div>")
  }
  if (this.tileObject.tileContext.chartType == "pie") {
    tileElement.append("<div class='legend' style='width:100%;height:auto;'></div>")
  }
  if (defaultPlusBtn) { // new row
    tileElement.insertBefore('.float-clear');
  }
  else { // remove row btn and add new div based on type
    customBtn.remove();
    $(".custom-btn-div").remove();
    $('.row-' + splitValue[1]).append(tileElement);
  }
  if (this.tileObject.tileContext.widgetType == "small") {
    tileElement.find(".settings").addClass('reduce-filter-size');
    tileElement.find(".filter").hide();
  }
  else if (this.tileObject.tileContext.widgetType == "medium") {
    tileElement.find(".widget-header").addClass('reduce-widget-header-size');
  }
  this.createGraph(this.tileObject, tileElement);
  this.triggerConfig(tileElement, this.tileObject); // add event for tile config
  this.triggerFilter(tileElement, this.tileObject);
  //this.triggerChildBtn(tileElement,this.tileObject);
  this.createTileData(this.tileObject);
  this.saveTileConfig(this.tileObject); // add event for tile save btn
};
