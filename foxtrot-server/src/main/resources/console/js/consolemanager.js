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

function TileFactory() {
}

TileFactory.create = function(type) {
    if (type === "donut") {
        return new DonutTile();
    } else if (type === "bar") {
        return new BarTile();
    } else if (type === "line") {
        return new LineTile();
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

function ConsoleManager(tileSet, queue, tables) {
    this.tileSet = tileSet;
    this.queue = queue;
    this.tables = tables;
}

ConsoleManager.prototype.getConsoleRepresentation = function () {
    var tiles = {};
    for (var tile in this.tileSet.currentTiles) {
        if (this.tileSet.currentTiles.hasOwnProperty(tile)) {
            var representation = this.tileSet.currentTiles[tile].getRepresentation();
            tiles[representation.id] = representation;
        }
    }
    var tileList = [];
    var tileDivs = $("#tileContainer").find(".tile");
    for (var i = 0; i < tileDivs.length; i++) {
        tileList.push(tileDivs[i].id);
    }
    representation = {
        tileList: tileList,
        tiles: tiles
    };
    if ($('#pinConsoleToApp').is(":checked")) {
        if (this.tables.selectedTable) {
            representation['appName'] = this.tables.selectedTable.name;
        }
    }
    return representation;
};

ConsoleManager.prototype.buildConsoleFromRepresentation = function (representation) {
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
    if (representation.hasOwnProperty('appName') && representation['appName']) {
        this.tables.forceSelectedTableAfterInit(representation['appName']);
        var tables = this.tables.tables;
        if (tables && tables.length > 0) {
            for (var i = 0; i < tables.length; i++) {
                if (tables[i].name === representation['appName']) {
                    var tableSelect = $("#tables");
                    tableSelect.val(i);
                    tableSelect.change();
                }
            }
        }
    }
    if (this.tables.selectedTable) {
        this.queue.executeCalls();
    }
};