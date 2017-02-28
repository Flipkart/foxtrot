function FoxTrot() {
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
	var tileId = guid();
	var object = {
		"id" : tileId,
		"title": title,
		"widgetType": widgetType,
		"table": table,
		"chartType": chartType
	}
	$("#addWidgetModal").modal('hide');

	var tile = TileFactory.create(object);
	var foxtrot = new FoxTrot();
	addTilesList(object);
};

$(document).ready(function(){
	var type = $("#widgetType").val();
	var foxtrot = new FoxTrot();
	$("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
	$("#default-btn").click(function () {
		console.log("clicked");
		defaultPlusBtn = true;
	});
});
