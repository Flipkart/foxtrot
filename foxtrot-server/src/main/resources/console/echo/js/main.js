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
var tiles = {};
var tileList = [];
var tileData = {};
var panelRow = [];
var globalData = [];
var defaultPlusBtn = true;
var customBtn;
var filterRowArray = [];
var currentChartType;
var tableList = [];
var currentFieldList = [];
var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
var interval = null;
function TablesView(id, tables) {
  this.id = id;
  this.tables = tables;
  this.tableSelectionChangeHandler = null;
}
TablesView.prototype.load = function (tables) {
  var select = $(this.id);
  select.find('option').remove();
  for (var i = tables.length - 1; i >= 0; i--) {
    select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
  }
  select.val(this.tables.getSelectionIndex());
  select.selectpicker('refresh');
  select.change();
};
function clearModalfields() { // used when modal table changed
  reloadDropdowns();
  removeFilters();
}
TablesView.prototype.registerTableSelectionChangeHandler = function (handler) {
  this.tableSelectionChangeHandler = handler;
};
TablesView.prototype.init = function () {
  this.tables.registerTableChangeHandler($.proxy(this.load, this));
  $(this.id).change($.proxy(function () {
    var value = parseInt($(this.id).val());
    var table = this.tables.tables[value];
    if (table) {
      if (this.tableSelectionChangeHandler) {
        this.tableSelectionChangeHandler(table.name);
      }
      this.tables.loadTableMeta(table);
      this.tables.selectedTable = table;
      console.log("Table changed to: " + table.name);
      //console.log(this);
    }
  }, this));
  this.tables.init();
};

function FoxTrot() {
  this.tables = new Tables();
  this.tablesView = new TablesView("#tileTable", this.tables);
  this.queue = new Queue();
  this.tableSelectionChangeHandler = null;
}
FoxTrot.prototype.init = function () {
  this.tablesView.registerTableSelectionChangeHandler($.proxy(function (value) {
    this.selectedTable = value;
    if (this.tableSelectionChangeHandler && value) {
      for (var i = this.tableSelectionChangeHandlers.length - 1; i >= 0; i--) {
        this.tableSelectionChangeHandlers[i](value);
      };
    }
  }, this));
  this.tablesView.init();
  this.queue.start();
};
function addTilesList(object) {
  tiles[object.id] = object;
  tileList.push(object.id);
}

function setClicketData(ele) {
  customBtn = ele;
  defaultPlusBtn = false;
  clearModal();
}
function renderTiles(object) {
  var tileFactory = new TileFactory();
  tileFactory.tileObject = object;
  tileFactory.create();
}

function getPeriodSelect(tileId) {
  return $("#" + tileId).find(".period-select").val();
}

FoxTrot.prototype.addTile = function () {
  var title = $("#tileTitle").val();
  var filterDetails = getFilters();
  var tableId = parseInt($("#tileTable").val());
  var table = this.tables.tables[tableId];
  var editTileId = $(".tileId").val();
  var tileId = guid();
  var isChild = $(".child-tile").val();
  var periodInterval = $("#period-select").val();
  isChild = (isChild == 'true');
  if ($("#tileTitle").val().length == 0 || !$("#tileTable").valid() || getWidgetType() == false) {
    $(".top-error").show();
    return;
  }
  if (getChartFormValues()[1] == false) {
    $(".top-error").show();
    return;
  }
  $(".top-error").hide();
  var widgetType = getWidgetType();
  if (!isChild && editTileId) tileId = editTileId;
  var context = {
    "widgetType": widgetType
    , "table": table.name
    , "editTileId": editTileId
    , "tableDropdownIndex": tableId
    , "chartType": currentChartType
    , "filters": filterDetails.length == 0 ? [] : filterDetails
    , "tableFields": currentFieldList
    , "periodInterval": periodInterval
  }
  context = $.extend({}, getChartFormValues()[0], context);
  var object = {
    "id": tileId
    , "title": title
    , "tileContext":context
  }
  console.log(object);
  var tileFactory = new TileFactory();
  currentChartType = "";
  if (!editTileId && !isChild) { // for new tile
    tileFactory.tileObject = object;
    tileFactory.create();
    var foxtrot = new FoxTrot();
    addTilesList(object);
  }
  else { // edit tile
    tileFactory.tileObject = object;
    tileFactory.updateTileData();
  }
  $("#addWidgetModal").modal('hide');
  removeFilters();
};
function addFitlers() {
  var filterCount = filterRowArray.length;
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row filters clearfix" id="filter-row-' + filterCount + '"><div class="col-md-3"><select class="selectpicker filter-column" data-live-search="true"><option>select</option></select></div><div class="col-md-3"><select class="selectpicker filter-type" data-live-search="true"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option></select></div><div class="col-md-4"><input type="text" class="form-control filter-value"></div><div class="col-md-2 filter-delete"><span class="glyphicon glyphicon-trash" aria-hidden="true"></span></div></div>';
  $(".add-filter-row").append(filterRow);
  var filterValueEl = $("#filter-row-" + filterCount).find('.filter-delete');
  var filterType = $("#filter-row-" + filterCount).find('.filter-type');
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-" + filterCount).find('.filter-column')
  generateDropDown(currentFieldList, filterColumn);
  $(filterValueEl).click(function () {
    deleteFilterRow(this);
  });
}
FoxTrot.prototype.addFilters = function () {
  addFitlers();
}

function showHideForms() {
  $("#table-units").hide();
  $("#table-units").find(".table-units-active").removeClass(".table-units-active");
}

function removeFilters() {
  $(".filters").remove();
  filterRowArray = [];
}

function clearModal() {
  $("#widgetType").val('');
  $("#tileTitle").val('');
  $(".tile-table").val('');
  $('.tile-table option').last().prop('selected', true);
  $(".tile-table").selectpicker('refresh');
  $(".tile-table").change();
  $("#tileTimeFrame").val('');
  $(".tile-time-unit").val('minutes');
  $(".tileId").val('');
  $(".vizualization-type").show();
  $(".vizualization-type").removeClass("vizualization-type-active");
  removeFilters();
  $("#table-units").hide();
}
FoxTrot.prototype.resetModal = function () {
  clearModal();
}

function invokeClearChartForm() {
  if (currentChartType == "line") {
    clearLineChartForm();
  }
  else if (currentChartType == "trend") {
    clearTrendChartForm();
  }
  else if (currentChartType == "stacked") {
    clearstackedChartForm();
  }
  else if (currentChartType == "radar") {
    clearRadarChartForm();
  }
  else if (currentChartType == "gauge") {
    clearGaugeChartForm();
  }
  else if (currentChartType == "stackedBar") {
    clearStackedBarChartForm();
  }
  else if (currentChartType == "pie") {
    clearPieChartForm();
  }
}
function clickedChartType(el) {
  // hide
  $("#table-units>div.table-units-active").removeClass("table-units-active");
  // show
  currentChartType = $(el).data('chartType');
  reloadDropdowns();
  invokeClearChartForm();
  $("#table-units").show();
  var chartDataEle = $("#table-units").find("#" + currentChartType + "-chart-data");
  if (chartDataEle.length > 0) {
    //$(chartDataEle).show();
    $(chartDataEle).addClass("table-units-active");
  }
  else {
    showHideForms();
  }
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(el).addClass("vizualization-type-active");
}

function saveConsole() {
  var name = "testing console"
  var representation = {
    id: name.trim().toLowerCase().split(' ').join("_")
    , updated: new Date().getTime()
    , name: name
    , sections: globalData
  };
  console.log(JSON.stringify(representation));
  /*$.ajax({
    url: apiUrl+("/foxtrot/v1/consoles"),
    type: 'POST',
    contentType: 'application/json',
    data: JSON.stringify(representation),
    success: function() {
      success("Saved console. The new console can be accessed <a href='?console=" + representation.id + "' class='alert-link'>here</a>");
    },
    error: function() {
      error("Could not save console");
    }
  })*/
}

function loadConsole() {
  /*$.ajax({
    url: hostDetails.url("/foxtrot/v1/consoles/" + consoleId),
    type: 'GET',
    contentType: 'application/json',
    success: $.proxy(this.consoleManager.buildConsoleFromRepresentation, this.consoleManager),
    error: function() {
      error("Could not save console");
    }
  })*/
}

function consoleTabs(evt, currentTab) {
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    if (tablinks[i].className.endsWith("active")) {
      var tabName = tablinks[i].id;
      var tempObject = {
        "tiles": tileList
        , "tileData": tileData
      }
      if (tileList.length > 0) {
        var deleteIndex = findIndex(tabName);
        if (deleteIndex >= 0) {
          globalData.splice(deleteIndex, 1);
          globalData.splice(deleteIndex, 0, {
            [tabName]: tempObject
          });
        }
        else {
          globalData.push({
            [tabName]: tempObject
          });
        }
      }
      clearModal();
      tileData = {};
      tileList = [];
    }
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  //document.getElementById(cityName).style.display = "block";
  $(".tile-container").empty();
  $(".tile-container").append('<div class="float-clear"></div>');
  evt.currentTarget.className += " active";
  var currentTabName = currentTab.toLowerCase();
  var tabIndex = findIndex(currentTabName);
  if (tabIndex >= 0) {
    var currentTabArray = globalData[tabIndex][currentTabName];
    tileList = currentTabArray.tiles;
    tileData = currentTabArray.tileData;
    for (var i = 0; i < tileList.length; i++) {
      renderTiles(tileData[tileList[i]]);
    }
  }
  clearInterval(interval);
}
$(document).ready(function () {
  var type = $("#widgetType").val();
  var foxtrot = new FoxTrot();
  $("#addWidgetModal").validator();
  $("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
  $("#filter-add-btn").click($.proxy(foxtrot.addFilters, foxtrot));
  $(".vizualization-type").click(function () {
    clickedChartType(this);
  });
  $("#default-btn").click(function () {
    defaultPlusBtn = true;
    foxtrot.resetModal();
    $(".settings-form").find("input[type=text], textarea").val("");
  });
  foxtrot.init();
  $("#saveConsole").click(function () {
    saveConsole();
  });
});
