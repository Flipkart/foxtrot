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
  console.log('tables')
  console.log(tables)
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
  console.log('initiated')
	this.tables.init();
};

function FoxTrot() {
  this.tables = new Tables();
  this.tablesView = new TablesView("#tileTable", this.tables);
  this.queue = new Queue();
  this.tableSelectionChangeHandler = null;
  //this.loadTableList()
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
	//this.tablesView.registerTableSelectionChangeHandler($.proxy(this.queue.executeCalls, this.queue));
};

var tiles = {};
var tileList = [];
var tileData = [];
var panelRow = [];
var defaultPlusBtn = true;
var customBtn;
var filterRowArray = [];
var currentChartType;

function addTilesList(object) {
	tiles[object.id] = object;
	tileList.push(object.id);
}

function setClicketData(ele) {
	customBtn = ele;
	defaultPlusBtn = false;
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

FoxTrot.prototype.addTile = function() {
	var widgetType = $("#widgetType").val();
	var title = $("#tileTitle").val();
	var tableId = parseInt($("#tileTable").val());
  var table = this.tables.tables[tableId];
	var chartType = currentChartType;
  var tileTimeFrame = $("#tileTimeFrame").val();
  var editTileId = $("#tileId").val();
  var period = $(".tile-time-unit").val();
	var tileId = guid();
  getFilters();
	var object = {
		"id" : tileId,
		"title": title,
		"widgetType": "full",
		"table": table.name,
		"chartType": currentChartType,
    "tileTimeFrame": tileTimeFrame,
    "editTileId": editTileId,
    "filters": getFilters(),
    "period": period
	}
  currentChartType = "";
  if(!editTileId) {// for new tile
    $("#addWidgetModal").modal('hide');
    var tile = TileFactory.create(object);
    var foxtrot = new FoxTrot();
    addTilesList(object);
  } else {// edit tile

  }
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

FoxTrot.prototype.prepareFieldOption = function(el) {
  $.each(this.tables.currentTableFieldMappings, function (i, item) {
    $(el).append($('<option>', {
        value: item.field,
        text : item.field
    }));
  });
  $(el).selectpicker('refresh');
}

FoxTrot.prototype.addFilters = function() {
  var filterCount = filterRowArray.length;
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row filters clearfix" id="filter-row-'+filterCount+'"><div class="col-md-3"><select class="filter-column" data-live-search="true"><option>select</option></select></div><div class="col-md-3"><select class="selectpicker filter-type" data-live-search="true"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option></select></div><div class="col-md-4"><input type="text" class="form-control filter-value"></div><div class="col-md-2 filter-delete"><span class="glyphicon glyphicon-trash" aria-hidden="true"></span></div></div>';
  $( ".add-filter-row" ).prepend(filterRow);
  $('.selectpicker').selectpicker('refresh');

  var filterValueEl = $("#filter-row-"+filterCount).find('.filter-delete');
  var filterColumn = $("#filter-row-"+filterCount).find('.filter-column')
  this.prepareFieldOption(filterColumn);
  $(filterValueEl).click( function() {
    deleteFilterRow(this);
  });
}

FoxTrot.prototype.resetModal = function() {
  $("#widgetType").val('');
	$("#tileTitle").val('');
	$("#tileTable").val('');
  $("#tileTimeFrame").val('');
  $(".tile-time-unit").val('minutes');
  filterRowArray = [];
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(".filters").remove();
}

function clickedChartType(el) {
  currentChartType = $(el).data('chartType');
  $("#table-units").show();
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
