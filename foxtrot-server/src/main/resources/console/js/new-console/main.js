function FoxTrot() {
}
var tileList = [];
var tileData = [];

function addTilesList(id) {
	tileList.push(id);
	console.log(tileList);
}

FoxTrot.prototype.addTile = function() {
	var type = $("#widgetType").val();
	var size = $("#widgetSize").val();
	$("#addWidgetModal").modal('hide');
	var tileId = guid();
	var tile = TileFactory.create({id: tileId, size: size});
	var foxtrot = new FoxTrot();
	addTilesList(tileId);
};

$(document).ready(function(){
	var type = $("#widgetType").val();
	var foxtrot = new FoxTrot();
	$("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
});
