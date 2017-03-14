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
}

Queue.prototype.enqueue = function (key, ajaxRequest) {
  console.log("Adding: " + key);
  this.requests[key] = ajaxRequest;
};

Queue.prototype.remove = function (key) {
  console.log("Removing: " + key);
  delete this.requests[key];
};

Queue.prototype.start = function () {
  setInterval($.proxy(this.executeCalls, this),this.refreshTime * 1000);
};

Queue.prototype.executeCalls = function () {
  for (var property in this.requests) {
    if (this.requests.hasOwnProperty(property)) {
          this.requests[property]();
    }
  }
};

function Tile() {
}

function TileFactory() {
  this.tileObject = "";
}

function submitClicked(e) {
	console.log('submit clicked');
}

function pushTilesObject(object) {
	tileData.push(object);
}

TileFactory.prototype.updateTileData = function() {
  var selectedTile = $("#"+this.tileObject.id);
	selectedTile.find(".tile-title").text(this.tileObject.title);
	var tileid= this.tileObject.id;
	var prepareTileData = { };
	prepareTileData[this.tileObject.id] = this.tileObject;
	pushTilesObject(prepareTileData);
  this.createGraph(this.tileObject, selectedTile);
}

TileFactory.prototype.createTileData = function(object) {
	var selectedTile = $("#"+object.id);
	selectedTile.find(".tile-title").text(object.title);
	var tileid= object.id;
	var prepareTileData = { };
	prepareTileData[object.id] = object;
	pushTilesObject(prepareTileData);
}

TileFactory.prototype.getTileFormValue = function(form, modal, object) {
	var tileFormValue = {};
	tileFormValue.title = form.find(".tile-title").val();
	tileFormValue.table = form.find(".tile-table").val();
	tileFormValue.id = form.find(".tileId").val();
	tileFormValue.timeUnit = form.find(".tile-time-unit").val();
	tileFormValue.timeValue = form.find(".tile-time-value").val();
	tileFormValue.chartType = form.find(".tile-chart-type").val();
	//updateTile(tileFormValue, modal);
}

function setConfigValue(object) {
	var form = $("#addWidgetModal").find("form");
	form.find(".tile-title").val(object.title);
	form.find(".tile-table").val(object.table);
	form.find(".tile-time-unit").val(object.timeUnit);
	form.find(".tile-time-value").val(object.timeValue);
	form.find(".tile-chart-type").val(object.chartType);
}

function clearForm() {
	var form = $("#tile-configuration").find("form");
	form.find(".tile-title").val('');
	form.find(".tile-table").val('');
	form.find(".tile-time-unit").val('');
	form.find(".tile-time-value").val('');
	form.find(".tile-chart-type").val('');
}

function newBtnElement() {
	return "<div class='col-md-2 custom-btn-div'><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn btn btn-primary filter-nav-button glyphicon glyphicon-plus custom-add-btn'onClick='setClicketData(this)'  data-toggle='modal' id='row-"+row+"'></button><div>"
}

// create new div
TileFactory.prototype.createNewRow = function(tileElement) {
	tileElement.addClass("col-md-12"); // add class for div which is full width
	if(panelRow.length == 0) { // initial page
		row = 1;
		panelRow.push({widgetType : this.tileObject.widgetType, id : this.tileObject.id});
		tileElement.addClass("row-"+row);
	} else { // incremetn row value by one
		panelRow.push({widgetType : this.tileObject.widgetType, id : this.tileObject.id});
		row = panelRow.length;
		tileElement.addClass("row-"+row);
	}
	if(this.tileObject.widgetType != "full") // dont add row add button for full widget
		tileElement.append(newBtnElement());
	return tileElement;
}

// Add click event for tile config icon
TileFactory.prototype.triggerConfig = function(tileElement, object) {
	tileElement.find(".widget-toolbox").find(".glyphicon-cog").click(function () {
	$("#addWidgetModal").modal('show');
	$("#addWidgetModal").find(".tileId").val(object.id);
	var tileListIndex = tileList.indexOf(object.id);
	var tileDataIndex = tileData[tileListIndex];
  var tileId = tileList[tileListIndex];
  var selectedTileObject = tileDataIndex[tileId];
    console.log(selectedTileObject);
    if(selectedTileObject) {
      setConfigValue(selectedTileObject);
    }
	});
}

// Save action for tile config save button
TileFactory.prototype.saveTileConfig = function(object) {
	$("#tile-configuration").find(".save-changes").click( function () {
    var form = $("#tile-configuration").find("form");
    form.off('submit');
    form.on('submit', $.proxy(function (e) {
			this.getTileFormValue(form, "tile-configuration", object)
	    $("#tile-configuration").modal('hide');
	    e.preventDefault();
    }, object));
	});
}

TileFactory.prototype.createGraph = function(object, tileElement) {
  if(object.chartType == "line") {
    var lineGraph = new LineTile();
		//lineGraph.render(tileElement, object);
    lineGraph.getQuery(tileElement, this.tileObject);
	} else if(object.chartType == "radar") {
    tileElement.find(".chart-item").append('<div id="radar-'+object.id+'" style="width:200;height:200"></div>');
    var radarGraph = new RadarTile();
		radarGraph.render(tileElement, object);
  }
}

TileFactory.prototype.create = function () {
	var tileElement = $(handlebars("#tile-template", {
		tileId: this.tileObject.id
		, title: this.tileObject.title
	}));
	var row = 0;// row
	var clickedRow;// clicked row
	if(defaultPlusBtn) { // check its new row
		tileElement = this.createNewRow(tileElement)
	} else {// row button action
		var splitValue = customBtn.id.split("-");
		var rowObject = panelRow[splitValue[1] - 1];
		clickedRow = rowObject.id
		if(this.tileObject.widgetType != rowObject.widgetType) {// f choosen type and row type is not equal
			tileElement = this.createNewRow(tileElement);
			defaultPlusBtn = true;
		}

		if(this.tileObject.widgetType == 'small'&& rowObject.widgetType == 'small') {
			var findElement = $("."+customBtn.id);
			if(findElement.find(".row-col-1").length == 0) {
				tileElement.addClass('row-col-1');
				tileElement.append("<div><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn btn btn-primary filter-nav-button glyphicon glyphicon-plus custom-add-btn row-col-1'onClick='setClicketData(this)'  data-toggle='modal' id='row-"+splitValue[1]+"'></button><div>");
			}
		}
	}

	if(this.tileObject.widgetType == "full") {
		tileElement.find(".tile").addClass('col-md-12');
	} else if(this.tileObject.widgetType == "medium") {
		tileElement.find(".tile").addClass('col-md-6');
		tileElement.find(".tile").width(590);
	} else if(this.tileObject.widgetType == "small") {
		tileElement.find(".tile").addClass('col-md-4');
		tileElement.find(".tile").width(300);
		tileElement.find(".tile").height(200);
	}

	tileElement.find(".chart-item").append('<div id="'+this.tileObject.id+'-health-text" class="lineGraph-health-text">10,000</div>');
	tileElement.find(".chart-item").append('<div id="'+this.tileObject.id+'-health" style=""></div>');
	tileElement.find(".chart-item").append('<div id="'+this.tileObject.id+'"></div>');

	if(defaultPlusBtn) {// new row
		tileElement.insertBefore('.float-clear');
	} else {// remove row btn and add new div based on type
		customBtn.remove();
		tileElement.insertAfter('#'+clickedRow);
	}

  this.createGraph(this.tileObject, tileElement);
	this.triggerConfig(tileElement, this.tileObject);// add event for tile config
	this.createTileData(this.tileObject);
	this.saveTileConfig(this.tileObject);// add event for tile save btn
};
