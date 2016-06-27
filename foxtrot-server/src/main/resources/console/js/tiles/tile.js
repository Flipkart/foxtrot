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
    console.log("tile: queue-constructor");
    this.requests = {};
    this.refreshTime = 5;
    this.timeout = 4000;
}

Queue.prototype.enqueue = function (key, ajaxRequest) {
    console.log("tile: queue-enqueue");
    console.log("Adding: " + key);
    this.requests[key] = ajaxRequest;
};

Queue.prototype.remove = function (key) {
    console.log("tile: queue-remove");
    console.log("Removing: " + key);
    delete this.requests[key];
};

Queue.prototype.start = function () {
    console.log("tile: queue-start");
    setInterval($.proxy(this.executeCalls, this), this.refreshTime * 1000);
};

Queue.prototype.executeCalls = function () {
    console.log("tile: queue-executeCalls");
    for (var property in this.requests) {
        if (this.requests.hasOwnProperty(property)) {
            this.requests[property]();
        }
    }
};


function Tile() {
    console.log("tile: constructor");
    this.id = null;
    this.type = null;
    this.tables = null;
    this.title = "";
    this.width = 0;
    this.height = 0;
    this.query = null;
    this.previousQuery = null;
    this.refresh = true;
    this.queue = null;
    this.cachedData = null;
    this.setupModalName = null;
    this.url = hostDetails.url("/foxtrot/v1/analytics");
    this.contentType = "application/json";
    this.httpMethod = "POST";
}

Tile.prototype.init = function (id, queue, tables) {
    console.log("tile: init");
    this.id = id;
    this.queue = queue;
    this.tables = tables;
    if (this.refresh) {
        queue.enqueue(this.id, $.proxy(this.reloadData, this));
    }
}

Tile.prototype.cleanup = function () {
    console.log("tile: cleanup");
    if (this.queue) {
        this.queue.remove(this.id);
    }
};

//Tile.prototype.reloadData = function () {
//    console.log("tile: reloadData");
//    this.query = this.getQuery();
//    this.previousQuery = this.getPreviousQuery();
//
//    if (!this.query) {
//        console.log("did not update since query setup is not complete " + this.id);
//        return;
//    }
//
//    $.ajax({
//        method: this.httpMethod,
//        dataType: 'json',
//        accepts: {
//            json: 'application/json'
//        },
//        url: this.url,
//        contentType: this.contentType,
//        timeout: this.queue.timeout,
//        data: this.query,
//        //success: $.proxy(this.newDataReceived, this)
//        success: $.proxy(this.newDataReceived, this)
//    });
//};

Tile.prototype.reloadData = function () {
    console.log("tile: reloadData");

    this.query = this.getQuery(0);
    this.previousQuery = this.getQuery(1);

    if (!this.query || !this.previousQuery) {
        console.log("query setup incomplete " + this.id);
        return;
    }

    if (this.getCompareStatus()) {
        $.when($.ajax(
            {                       // New Code
                method: this.httpMethod,
                dataType: 'json',
                accepts: {
                    json: 'application/json'
                },
                url: this.url,
                contentType: this.contentType,
                timeout: this.queue.timeout,
                data: this.query

            }), $.ajax(
            {
                method: this.httpMethod,
                dataType: 'json',
                accepts: {
                    json: 'application/json'
                },
                url: this.url,
                contentType: this.contentType,
                timeout: this.queue.timeout,
                data: this.previousQuery

            })).done($.proxy(this.newDataReceived, this));
    } else {
        $.ajax({
            method: this.httpMethod,
            dataType: 'json',
            accepts: {
                json: 'application/json'
            },
            url: this.url,
            contentType: this.contentType,
            timeout: this.queue.timeout,
            data: this.query,
            //success: $.proxy(this.newDataReceived, this)
            success: $.proxy(this.newDataReceived, this)
        });
    }
};

Tile.prototype.newDataReceived = function (data, dataPrevious) {
    //this.cachedData = data;

    if (dataPrevious === "success") {
        this.render(data, true);
    } else {
        this.renderWithCompare(data[0], dataPrevious[0], true);
    }
};

Tile.prototype.handleResize = function (event, ui) {
    console.log("tile: handleResize");
    this.render(this.cachedData, false);
};

Tile.prototype.getRepresentation = function () {
    console.log("tile: getRepresentation");
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
};

Tile.prototype.loadTileFromRepresentation = function (representation) {
    console.log("tile: loadTileFromRepresentation");
    this.id = representation.id;
    this.width = representation.width;
    this.height = representation.height;
    this.title = representation.title;
    this.loadSpecificData(representation);
}

Tile.prototype.isSetupDone = function () {
    console.log("tile: isSetupDone");
    return false;
};

Tile.prototype.render = function (data, animate) {
    console.log("tile: render");
};

Tile.prototype.configChanged = function () {
    console.log("tile: configChanged");
    console.log("Widget config changed");
};

Tile.prototype.getQuery = function () {
    console.log("tile: getQuery");
    return this.query;
}

Tile.prototype.populateSetupDialog = function () {
    console.log("tile: populateSetupDialog");
};

Tile.prototype.registerSpecificData = function (representation) {
    console.log("tile: registerSpecificData");
    console.log("Base representation called for " + this.typeName + ": " + this.id);
};

Tile.prototype.loadSpecificData = function (representation) {
    console.log("tile: loadSpecificData");
    console.log("Base load called for " + this.typeName + ": " + this.id)
};

Tile.prototype.registerComplete = function () {
    console.log("tile: registerComplete");
}

Tile.prototype.getUniqueValues = function () {
    console.log("tile: getUniqueValues");
}

Tile.prototype.filterValues = function (values) {
    console.log("tile: filterValues");
}

Tile.prototype.getCompareStatus = function() {
    console.log("tile: getCompareStatus");
};



function TileSet(id, tables) {
    console.log("tileSet: constructor");
    this.id = id;
    this.tables = tables;
    this.currentTiles = {};
}

TileSet.prototype.closeHandler = function (eventData) {
    console.log("tileSet: closeHandler");
    var tileId = eventData.currentTarget.parentNode.parentNode.parentNode.getAttribute('id');
    this.unregister(tileId);
    var tileContainer = $(this.id);
    if (tileContainer.find(".tile").length == 0) {
        tileContainer.find(".removable-text").css("display", "block");
    }
};

TileSet.prototype.register = function (tile) {
    console.log("tileSet: register");
    var tileContainer = $(this.id);
    this.currentTiles[tile.id] = tile;
    var newDiv = $(handlebars("#tile-template", {tileId: tile.id, title: tile.title}));
    tileContainer.find(".removable-text").css("display", "none");
    newDiv.insertBefore('.float-clear');
    $(newDiv).resizable();
    if (tile.height != 0 && tile.width != 0) {
        newDiv.height(tile.height);
        newDiv.width(tile.width);
    }
    newDiv.find(".widget-toolbox").find(".glyphicon-remove").click($.proxy(this.closeHandler, this));
    newDiv.find(".widget-toolbox").find(".glyphicon-cog").click(function () {
        if (tile.setupModalName) {
            var modal = null;
            modal = $(tile.setupModalName);
            modal.find(".tileId").val(tile.id);
            var form = modal.find("form");
            form.off('submit');
            form.on('submit', $.proxy(function (e) {
                this.configChanged();
                $(this.setupModalName).modal('hide');
                if (tile.isSetupDone()) {
                    $("#" + tile.id).removeClass("tile-drag");
                }
                e.preventDefault();
            }, tile));
            tile.populateSetupDialog();
            modal.modal('show');
        }
    });
    newDiv.find(".widget-toolbox").find(".glyphicon-fullscreen").click($.proxy(function () {
        launchIntoFullscreen(document.getElementById("content-for-" + this.id));
    }, tile));
    newDiv.find(".widget-toolbox").find(".glyphicon-filter").click(function () {
        var modal = $("#setupFiltersModal");
        modal.find(".tileId").val(tile.id);
        var fv = $("#filter_values");
        fv.multiselect('dataprovider', tile.getUniqueValues());
        fv.multiselect('refresh');
        var form = modal.find("form");
        form.off('submit');
        form.on('submit', $.proxy(function (e) {
            this.filterValues($("#filter_values").val());
            $("#setupFiltersModal").modal('hide');
            e.preventDefault();
        }, tile));
        tile.populateSetupDialog();
        modal.modal('show');
    });

    if (!tile.isSetupDone()) {
        newDiv.addClass("tile-drag");
    }
    tile.registerComplete();

};

TileSet.prototype.unregister = function (tileId) {
    console.log("tileSet: unregister");
    var tile = this.currentTiles[tileId];
    if (!tile) {
        return;
    }
    $("#" + tileId).remove();
    tile.cleanup();
    delete this.currentTiles[tileId];
};




function GenericTile() {
    console.log("GenericTile: constructor");
    this.query = JSON.stringify({
        opcode: "group",
        table: "abcd",
        nesting: ["header.configName", "data.name"]
    });
    this.refresh = true;
}

GenericTile.prototype = new Tile();




function TileFactory() {
    console.log("tileFactory: constructor");
}

TileFactory.create = function (type) {
    console.log("tileFactory: create");
    if (type === "donut") {
        return new DonutTile();
    } else if (type === "bar") {
        return new BarTile();
    } else if (type === "histogram") {
        return new Histogram();
    } else if (type === "eventbrowser") {
        return new EventBrowser();
    } else if (type === "stacked_bar") {
        return new StackedBar();
    } else if (type === "statstrend") {
        return new StatsTrend();
    } else if (type === 'fql_table') {
        return new FqlTable()
    }
};
