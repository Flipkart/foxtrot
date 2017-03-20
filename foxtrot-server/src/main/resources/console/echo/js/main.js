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

function TablesView(id, tables) {
	this.id = id;
	this.tables = tables;
	this.tableSelectionChangeHandler = null;
}

TablesView.prototype.load = function(tables) {
	var select = $(this.id);
	select.find('option')
    .remove();
	for (var i = tables.length - 1; i >= 0; i--) {
		select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
	}
	select.val(this.tables.getSelectionIndex());
	select.selectpicker('refresh');
	select.change();
};

function uniqueKey(fields, element) {
  console.log('triggered')
  var el = $(element);
  console.log(el);
  el.find('option').remove();
  $(el).append($('<option>', {
        value: "none",
        text : "none"
    }));
	$.each(fields, function (i, item) {
    $(el).append($('<option>', {
        value: item.field,
        text : item.field
    }));
  });
  $(el).selectpicker('refresh');
}

TablesView.prototype.registerTableSelectionChangeHandler = function(handler) {
	this.tableSelectionChangeHandler = handler;
};

TablesView.prototype.init = function() {
	this.tables.registerTableChangeHandler($.proxy(this.load, this));
	$(this.id).change($.proxy(function(){
		var value = parseInt($(this.id).val());
		var table = this.tables.tables[value];
		if(table) {
			if(this.tableSelectionChangeHandler) {
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

FoxTrot.prototype.init = function() {
	this.tablesView.registerTableSelectionChangeHandler($.proxy(function(value){
		this.selectedTable = value;
		if(this.tableSelectionChangeHandler && value) {
			for (var i = this.tableSelectionChangeHandlers.length - 1; i >= 0; i--) {
				this.tableSelectionChangeHandlers[i](value);
			};
		}
	}, this));
	this.tablesView.init();
	this.queue.start();
};

var tiles = {};
var tileList = [];
var tileData = [];
var panelRow = [];
var defaultPlusBtn = true;
var customBtn;
var filterRowArray = [];
var currentChartType;
var tableList = [];
var currentFieldList = [];

function addTilesList(object) {
	tiles[object.id] = object;
	tileList.push(object.id);
}

function setClicketData(ele) {
	customBtn = ele;
	defaultPlusBtn = false;
  clearModal();
}

function getFilters() {
  var filterDetails = [];
  for(var filter = 0; filter < filterRowArray.length; filter++) {
    var filterId = filterRowArray[filter];
    var el = $("#filter-row-"+filterId);
    var filterColumn = $(el).find(".filter-column").val();
    var filterType = $(el).find(".filter-type").val();
    var filterValue = $(el).find(".filter-value").val();
    var filterObject = {"operator" : filterType, "value": filterValue, "field": filterColumn};
    filterDetails.push(filterObject);
  }
  return filterDetails;
}

function removeFilters () {
  $(".filters").remove();
}

function getWidgetType() {
  if(currentChartType == "line") {
    return "full";
  } else if(currentChartType == "radar") {
    return "medium";
  } else if(currentChartType == "gauge" || currentChartType == "trend") {
    return "small";
  }
}

function getLineChartFormValues() {
  var period = $(".tile-time-unit").val();
  var uniqueCount = $("#uniqueKey").val();
  var periodValue = $("#periodValue").val();
  return {
    "period": period,
    "uniqueCountOn": uniqueCount,
    "periodValue": periodValue,
  }
}

function getTrendChartFormValues() {
  var period = $(".trend-time-unit").val();
  var statsField = $(".stats-field").val();
  var statsToPlot = $(".statistic_to_plot").val();
  var statsPeroidValue = $("#stats-period-value").val();
  return {
    "period": period,
    "statsFieldName": "eventData.amount",
    "statsToPlot": statsToPlot,
  }
}

function getChartFormValues() {
  if(currentChartType == "line") {
    return getLineChartFormValues();
  } else if(currentChartType == "trend") {
    return getTrendChartFormValues();
  }
}

FoxTrot.prototype.addTile = function() {
	var title = $("#tileTitle").val();
  var filterDetails = getFilters();
	var tableId = parseInt($("#tileTable").val());
  var table = this.tables.tables[tableId];
  var editTileId = $(".tileId").val();
	var tileId = guid();
  var isChild = $(".child-tile").val();
  isChild = (isChild == 'true');
  var widgetType = getWidgetType();
  if(!isChild && editTileId)
    tileId = editTileId;
	var queryValues = {
		"id" : tileId,
		"widgetType": widgetType,
		"table": table.name,
    "editTileId": editTileId,
    "tableDropdownIndex": tableId,
    "title" : title,
    "chartType": currentChartType,
    "filters": filterDetails.length == 0 ? [] : filterDetails,
  };
  var object = $.extend( {}, getChartFormValues(), queryValues );
  var tileFactory = new TileFactory();
  currentChartType = "";
  if(!editTileId && !isChild) {// for new tile
    tileFactory.tileObject = object;
    tileFactory.create();
    var foxtrot = new FoxTrot();
    addTilesList(object);
  } else {// edit tile
    tileFactory.tileObject = object;
    tileFactory.updateTileData();
  }
  $("#addWidgetModal").modal('hide');
  filterRowArray = [];
  removeFilters();
};

function deleteFilterRow(el) {
  var parentRow = $(el).parent();
  var parentRowId = parentRow.attr('id');
  var getRowId = parentRowId.split('-');
  var rowId = getRowId[2];
  filterRowArray = jQuery.grep(filterRowArray, function(value) {
    return value != rowId;
  });
  $(parentRow).remove();
}


function setFilters(object) {
  for(var filter = 0; filter < filterRowArray.length; filter++) {
    var filterId = filterRowArray[filter];
    var el = $("#filter-row-"+filterId);
    var fieldDropdown = $(el).find(".filter-column").val(object[filter].field);
    var operatorDropdown = $(el).find(".filter-type").val(object[filter].operator);
    $(el).find(".filter-value").val(object[filter].value);
    $(fieldDropdown).selectpicker('refresh');
    $(operatorDropdown).selectpicker('refresh');
  }
}

function addFitlers() {
  var filterCount = filterRowArray.length;
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row filters clearfix" id="filter-row-'+filterCount+'"><div class="col-md-3"><select class="selectpicker filter-column" data-live-search="true"><option>select</option></select></div><div class="col-md-3"><select class="selectpicker filter-type" data-live-search="true"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option></select></div><div class="col-md-4"><input type="text" class="form-control filter-value"></div><div class="col-md-2 filter-delete"><span class="glyphicon glyphicon-trash" aria-hidden="true"></span></div></div>';
  $( ".add-filter-row" ).prepend(filterRow);

  var filterValueEl = $("#filter-row-"+filterCount).find('.filter-delete');
  var filterType = $("#filter-row-"+filterCount).find('.filter-type');
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-"+filterCount).find('.filter-column')
  prepareFieldOption(filterColumn, currentFieldList);
  $(filterValueEl).click( function() {
    deleteFilterRow(this);
  });
}

function prepareFieldOption(el, currentFieldList) {
  $.each(currentFieldList, function (i, item) {
    $(el).append($('<option>', {
        value: item.field,
        text : item.field
    }));
  });
  $(el).selectpicker('refresh');
}

FoxTrot.prototype.addFilters = function() {
  addFitlers();
}

function filterUnits () {
  $("#table-units").hide();
}

function clearModal() {
  $("#widgetType").val('');
	$("#tileTitle").val('');
	$(".tile-table").val('');
  $('.tile-table option').last().prop('selected',true);
  $(".tile-table").selectpicker('refresh');
  $(".tile-table").change();
  $("#tileTimeFrame").val('');
  $(".tile-time-unit").val('minutes');
  $(".tileId").val('');
  filterRowArray = [];
  $(".vizualization-type").show();
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(".filters").remove();
  $("#table-units").hide();
}

FoxTrot.prototype.resetModal = function() {
  clearModal();
}

function clearLineChartForm () {
  var parentElement = $("#"+currentChartType+"-chart-data");
  var timeUnitEl = parentElement.find(".tile-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');
  var uniqeKey = parentElement.find("#uniqueKey");
  uniqeKey.find('option:eq(0)').prop('selected', true);
  $(uniqeKey).selectpicker('refresh');
  parentElement.find("#stats-periodValue").val('');
}

function clearTrendChartForm() {
  var parentElement = $("#"+currentChartType+"-chart-data");

  var timeUnitEl = parentElement.find(".trend-time-unit");
  timeUnitEl.find('option:eq(0)').prop('selected', true);
  $(timeUnitEl).selectpicker('refresh');

  var statsFieldEl = parentElement.find(".stats-field");
  statsFieldEl.find('option:eq(0)').prop('selected', true);
  $(statsFieldEl).selectpicker('refresh');

  var statsToPlot = parentElement.find(".statistic_to_plot");
  statsToPlot.find('option:eq(0)').prop('selected', true);
  $(statsToPlot).selectpicker('refresh');

  parentElement.find("#periodValue").val('');
  parentElement.find(".ignored-digits").val('');
}

function invokeClearChartForm() {
  if(currentChartType == "line") {
    clearLineChartForm();
  } else if(currentChartType == "trend") {
    clearTrendChartForm();
  }
}

function clickedChartType(el) {
  // hide
  var previousChartType = currentChartType;
  $("#table-units").find("#"+previousChartType+"-chart-data").hide();

  // show
  currentChartType = $(el).data('chartType');
  if(currentChartType == "line") {
    uniqueKey(currentFieldList, "#uniqueKey");
  } else if(currentChartType == "trend") {
    uniqueKey(currentFieldList, ".stats-field");
  }

  invokeClearChartForm();
  $("#table-units").show();
  var chartDataEle = $("#table-units").find("#"+currentChartType+"-chart-data");
  if(chartDataEle.length > 0) {
    $(chartDataEle).show();
  } else {
    filterUnits();
  }
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(el).addClass("vizualization-type-active");
}

$(document).ready(function(){
	var type = $("#widgetType").val();
	var foxtrot = new FoxTrot();
  $("#addWidgetModal").validator();
	$("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
  $("#filter-add-btn").click($.proxy(foxtrot.addFilters, foxtrot));
  $(".vizualization-type").click(function(){
    clickedChartType(this);
  });
	$("#default-btn").click(function () {
		defaultPlusBtn = true;
    foxtrot.resetModal();
    $(".settings-form").find("input[type=text], textarea").val("");
	});
  foxtrot.init();
});
