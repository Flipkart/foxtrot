function FoxTrot() {
}
var tileList = [];
var tileData = [];
var panelRow = [];
var defaultPlusBtn = true;
var customBtn;
function addTilesList(id) {
	tileList.push(id);
	console.log(tileList);
}

function setClicketData(ele) {
	customBtn = ele;
	defaultPlusBtn = false;
}

FoxTrot.prototype.addTile = function() {
	var type = $("#widgetType").val();
	$("#addWidgetModal").modal('hide');
	var tileId = guid();
	var tile = TileFactory.create({id: tileId, type: type});
	var foxtrot = new FoxTrot();
	addTilesList(tileId);
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
