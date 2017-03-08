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

function FoxTrot() {
  this.tables = new Tables();
  this.tables.init();
  this.tableSelectionChangeHandler = null;
  this.loadTableList(this.tables.tables)
  console.log('3');
  console.log(this.tables);
}
var tiles = {};
var tileList = [];
var tileData = [];
var panelRow = [];
var defaultPlusBtn = true;
var customBtn;
var filterRowArray = [];
var currentChartType;
var currentTableFields;

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
    var filterObject = {"operator" : filterType, "value": filterValue};
    filterDetails.push(filterObject);
  }
  return filterDetails;
}

FoxTrot.prototype.addTile = function() {
	var widgetType = $("#widgetType").val();
	var title = $("#tileTitle").val();
	var table = $("#tileTable").val();
	var chartType = $("#tileChartType").val();
  var tileTimeFrame = $("#tileTimeFrame").val();
  var editTileId = $("#tileId").val();
	var tileId = guid();
  getFilters();
	var object = {
		"id" : tileId,
		"title": title,
		"widgetType": "full",
		"table": table,
		"chartType": currentChartType,
    "tileTimeFrame": tileTimeFrame,
    "editTileId": editTileId,
    "filters": getFilters()
	}
  console.log(object);
  currentChartType = "";
  if(!editTileId) {// for new tile
    $("#addWidgetModal").modal('hide');
    var tile = TileFactory.create(object);
    var foxtrot = new FoxTrot();
    addTilesList(object);
  } else {// edit tile

  }
};

FoxTrot.prototype.setSelectedTable = function() {
  var tableValue = $("#tileTable").val();
  //this.loadTableMeta(this.tables.tables[tableValue]);
  var table = this.tables.tables[tableValue];
  $.ajax({
        url: "http://foxtrot.traefik.prod.phonepe.com/foxtrot/v1/tables/" + table.name + "/fields",
        contentType: "application/json",
        context: this,
        success: $.proxy(function (data) {
            currentTableFields = data.mappings;
        })
    });
}

FoxTrot.prototype.loadTableMeta = function(table) {
  this.tables.loadTableMeta(table);
  console.log(table);
}

FoxTrot.prototype.loadTableList = function(tables) {
  var select = $("#tileTable");
	select.find('option').remove();
  select.append("<option value=''>Select table</option>");
	for (var i = tables.length - 1; i >= 0; i--) {
		select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
	}
}

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

function prepareFieldOption (el) {
//  console.log(el)
//	el.find('option').remove();
//  el.append("<option value=''>Select column</option>");
//	for (var i = currentTableFields.length - 1; i >= 0; i--) {
//		el.append("<option value='" + i + "'>" + currentTableFields[i].field + '</option>');
//	}
//  $('.selectpicker').selectpicker('refresh');

  $.each(currentTableFields, function (i, item) {
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
  var filterRow = '<div class="row filters clearfix" id="filter-row-'+filterCount+'"><div class="col-md-3"><select class="filter-column"><option>select</option></select></div><div class="col-md-3"><select class="selectpicker filter-type"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option></select></div><div class="col-md-4"><input type="text" class="form-control filter-value"></div><div class="col-md-2 filter-delete"><span class="glyphicon glyphicon-trash" aria-hidden="true"></span></div></div>';
  $( ".add-filter-row" ).prepend(filterRow);
  $('.selectpicker').selectpicker('refresh');

  var filterValueEl = $("#filter-row-"+filterCount).find('.filter-delete');
  var filterColumn = $("#filter-row-"+filterCount).find('.filter-column')
  prepareFieldOption(filterColumn);
  $(filterValueEl).click( function() {
    deleteFilterRow(this);
  });
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
    $(".settings-form").find("input[type=text], textarea").val("");
	});
  $("#tileTable").change($.proxy(foxtrot.setSelectedTable, foxtrot))
});
