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
  this.loadTableList(this.tables.tables)
}
var tiles = {};
var tileList = [];
var tileData = [];
var panelRow = [];
var defaultPlusBtn = true;
var customBtn;

function addTilesList(object) {
	tiles[object.id] = object;
	tileList.push(object.id);
}

function setClicketData(ele) {
	customBtn = ele;
	defaultPlusBtn = false;
}

FoxTrot.prototype.addTile = function() {
	var widgetType = $("#widgetType").val();
	var title = $("#tileTitle").val();
	var table = $("#tileTable").val();
	var chartType = $("#tileChartType").val();
  var tileTimeFrame = $("#tileTimeFrame").val();
  var editTileId = $("#tileId").val();
	var tileId = guid();
	var object = {
		"id" : tileId,
		"title": title,
		"widgetType": widgetType,
		"table": table,
		"chartType": chartType,
    "tileTimeFrame": tileTimeFrame,
    "editTileId": editTileId
	}

  if(!editTileId) {// for new tile
    $("#addWidgetModal").modal('hide');
    var tile = TileFactory.create(object);
    var foxtrot = new FoxTrot();
    addTilesList(object);
  } else {// edit tile

  }
};

FoxTrot.prototype.loadTableList = function(tables) {
  var select = $("#tileTable");
	select.find('option').remove();
  select.append("<option value=''>Select table</option>");
	for (var i = tables.length - 1; i >= 0; i--) {
		select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
	}
}

$(document).ready(function(){
	var type = $("#widgetType").val();
	var foxtrot = new FoxTrot();
	$("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
	$("#default-btn").click(function () {
		defaultPlusBtn = true;
    $(".settings-form").find("input[type=text], textarea").val("");
	});
});
