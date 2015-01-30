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
};

Queue.prototype.enqueue = function(key, ajaxRequest) {
	console.log("Adding: " + key);
	this.requests[key] = ajaxRequest;
};

Queue.prototype.remove = function(key) {
	console.log("Removing: " + key);
	delete this.requests[key];
};

Queue.prototype.start = function() {
	setInterval($.proxy(this.executeCalls, this),
	                    this.refreshTime * 1000);
};

Queue.prototype.executeCalls = function(){
    for (var property in this.requests) {
        if (this.requests.hasOwnProperty(property)) {
            this.requests[property]();
        }
    }
};

function Tile() {
	this.id = null;
	this.type = null;
	this.tables = null;
	this.title = "";
	this.width = 0;
	this.height = 0;
	this.query = null;
	this.refresh = true;
	this.queue = null;
	this.cachedData = null;
	this.setupModalName = null;
	this.url = hostDetails.url("/foxtrot/v1/analytics");
	this.contentType = "application/json";
	this.httpMethod = "POST";
}

Tile.prototype.isSetupDone = function() {
	return false;
};

Tile.prototype.init = function(id, queue, tables) {
	this.id = id;
	this.queue = queue;
	this.tables = tables;
	if(this.refresh) {
		queue.enqueue(this.id, $.proxy(this.reloadData, this));
	}
}

Tile.prototype.cleanup = function() {
	if(this.queue) {
		this.queue.remove(this.id);
	}
};

Tile.prototype.reloadData = function() {
	this.query = this.getQuery();
	if(!this.query) {
		console.log("Did not update for: " + this.id);
		return;
	}
	$.ajax({
		method: this.httpMethod,
        dataType: 'json',
        accepts: {
            json : 'application/json'
        },
        url: this.url,
        contentType: this.contentType,
		timeout: this.queue.timeout,
		data: this.query,
		success: $.proxy(this.newDataReceived, this)
	});
};

Tile.prototype.newDataReceived = function(data) {
	this.cachedData = data;
	this.render(data, true);
};

Tile.prototype.render = function(data, animate) {
	console.log(data);	
};

Tile.prototype.handleResize = function(event, ui) {
	//console.log(ui);
	this.render(this.cachedData, false);
};

Tile.prototype.configChanged = function() {
	console.log("Widget config changed");
};

Tile.prototype.getQuery = function() {
	return this.query;
}

Tile.prototype.populateSetupDialog = function() {
	
};

Tile.prototype.getRepresentation = function() {
	var chartAreaId = "#content-for-" + this.id;
	var chartContent = $("#" + this.id).find(chartAreaId);
	this.height = chartContent.parent().height();
	this.width = chartContent.parent().width();
	var representation = {
		id: this.id,
		typeName: this.typeName,
		width: this.width,
		height: this.height,
		title: this.title
	};
	this.registerSpecificData(representation);
	return representation;
}

Tile.prototype.loadTileFromRepresentation = function(representation) {
	this.id = representation.id;
	this.width = representation.width;
	this.height = representation.height;
	this.title = representation.title;
	this.loadSpecificData(representation);
}

Tile.prototype.registerSpecificData = function(representation) {
	console.log("Base representation called for " + this.typeName + ": " + this.id);
};

Tile.prototype.loadSpecificData = function(representation) {
	console.log("Base load called for " + this.typeName + ": " + this.id)
};

Tile.prototype.registerComplete = function() {
}

Tile.prototype.getUniqueValues = function() {
}

Tile.prototype.filterValues = function(values) {
}

function TileSet(id, tables) {
	this.id = id;
	this.tables = tables;
	this.currentTiles = new Object();
};

TileSet.prototype.closeHandler = function(eventData) {
	var tileId = eventData.currentTarget.parentNode.parentNode.parentNode.getAttribute('id');
	this.unregister(tileId);
	var tileContainer = $(this.id);
	if(tileContainer.find(".tile").length  == 0) {
    	tileContainer.find(".removable-text").css("display", "block");
	}
};

TileSet.prototype.register = function(tile) {
	var tileContainer = $(this.id);
	this.currentTiles[tile.id] = tile;
	var newDiv = $(handlebars("#tile-template", {tileId: tile.id, title: tile.title}));
	tileContainer.find(".removable-text").css("display", "none");
	newDiv.insertBefore('.float-clear');
	if(tile.height != 0 && tile.width != 0) {
		newDiv.height(tile.height);
		newDiv.width(tile.width);
	}
	newDiv.find(".widget-toolbox").find(".glyphicon-remove").click($.proxy(this.closeHandler, this));
	newDiv.find(".widget-toolbox").find(".glyphicon-cog").click(function(){
		if(tile.setupModalName) {
			var modal = null;
			modal = $(tile.setupModalName);
			modal.find(".tileId").val(tile.id);
			var form = modal.find("form");
			form.off('submit');
			form.on('submit', $.proxy(function(e) {
					this.configChanged();
					$(this.setupModalName).modal('hide');
					if(tile.isSetupDone()) {
						$("#" + tile.id).removeClass("tile-drag");
					}
					e.preventDefault();
				}, tile));
			tile.populateSetupDialog();
			modal.modal('show');
		}
	});
	newDiv.find(".widget-toolbox").find(".glyphicon-fullscreen").click($.proxy(function(){
	    launchIntoFullscreen(document.getElementById("content-for-" + this.id));
	}, tile));
    newDiv.find(".widget-toolbox").find(".glyphicon-filter").click(function() {
        var modal = $("#setupFiltersModal");
        modal.find(".tileId").val(tile.id);
        var fv = $("#filter_values");
        fv.multiselect('dataprovider', tile.getUniqueValues());
        fv.multiselect('refresh');
        var form = modal.find("form");
        form.off('submit');
        form.on('submit', $.proxy(function(e) {
            this.filterValues($("#filter_values").val());
            $("#setupFiltersModal").modal('hide');
            e.preventDefault();
        }, tile));
        tile.populateSetupDialog();
        modal.modal('show');
    });

	if(!tile.isSetupDone()) {
		newDiv.addClass("tile-drag");
	}
    tile.registerComplete();

};

TileSet.prototype.unregister = function(tileId) {
	var tile = this.currentTiles[tileId];
	if(!tile) {
		return;
	}
	$("#" + tileId).remove();
	tile.cleanup();
	delete this.currentTiles[tileId];
};

function GenericTile() {
	this.query = JSON.stringify({
		opcode : "group",
		table : "abcd",
		nesting : [ "header.configName", "data.name"]
	});
	this.refresh = true;
}

GenericTile.prototype = new Tile();