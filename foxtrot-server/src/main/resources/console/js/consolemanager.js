function TileFactory() {
}

TileFactory.create = function(type) {
	if(type === "donut") {
		return new DonutTile();
	}
	else if(type === "bar") {
		return new BarTile();
	}
	else if(type === "line") {
		return new LineTile();
	}
	else if(type === "histogram") {
		return new Histogram();
	}
	else if(type === "eventbrowser") {
		return new EventBrowser();
	}
}

function ConsoleManager (tileSet, queue, tables) {
	this.tileSet = tileSet;
	this.queue = queue;
	this.tables = tables;
}

ConsoleManager.prototype.getConsoleRepresentation = function() {
	var tiles = new Object();
	for(tile in this.tileSet.currentTiles) {
		if(this.tileSet.currentTiles.hasOwnProperty(tile)) {
			var representation = this.tileSet.currentTiles[tile].getRepresentation();
			tiles[representation.id] = representation;
		}
	}
	var tileList = [];
	var tileDivs = $("#tileContainer").find(".tile");
	for (var i = 0; i < tileDivs.length; i++) {
		tileList.push(tileDivs[i].id);
	}
	var representation = {
		tileList: tileList,
		tiles: tiles
	}
	return representation;
}

ConsoleManager.prototype.buildConsoleFromRepresentation = function(representation) {
	var tiles = representation.tiles;
	var tileList = representation.tileList;
	for (var i = 0; i < tileList.length; i++) {
		var tileId = tileList[i];
		var tileRepresentation = tiles[tileId];
		var tile = TileFactory.create(tileRepresentation.typeName);
		tile.loadTileFromRepresentation(tileRepresentation);
		tile.init(tileRepresentation.id, this.queue, this.tables);
		this.tileSet.register(tile);
	}
	var modal = $("#saveConsoleModal");
	modal.find(".console-name").val(representation.name);
	$(".console-name").text(representation.name);
}