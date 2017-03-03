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

function Tile() {
}

function submitClicked(e) {
	console.log('submit clicked');
}

function pushTilesObject(object) {
	tileData.push(object);
}

function updateTile(object) {
	var selectedTile = $("#"+object.id);
	selectedTile.find(".tile-title").text(object.title);
	var tileid= object.id;
	var prepareTileData = { };
	prepareTileData[object.id] = object;
	pushTilesObject(prepareTileData);
}

function getTileFormValue(form, modal, object) {
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
function createNewRow(newDiv, object) {
	newDiv.addClass("col-md-12"); // add class for div which is full width
	if(panelRow.length == 0) { // initial page
		row = 1;
		panelRow.push({widgetType : object.widgetType, id : object.id});
		newDiv.addClass("row-"+row);
	} else { // incremetn row value by one
		panelRow.push({widgetType : object.widgetType, id : object.id});
		row = panelRow.length;
		newDiv.addClass("row-"+row);
	}
	if(object.widgetType != "full") // dont add row add button for full widget
		newDiv.append(newBtnElement());
	return newDiv;
}

// Add click event for tile config icon
function triggerConfig(newDiv, object) {
	newDiv.find(".widget-toolbox").find(".glyphicon-cog").click(function () {
	$("#addWidgetModal").modal('show');
	$("#addWidgetModal").find(".tileId").val(object.id);
	var tileListIndex = tileList.indexOf(object.id);
	var tileDataIndex = tileData[tileListIndex];
  var tileId = tileList[tileListIndex];
  var selectedTileObject = tileDataIndex[tileId];
    if(selectedTileObject) {
      setConfigValue(selectedTileObject);
    }
	});
}

function lineGraph(newDiv, object) {

}

// Save action for tile config save button
function saveTileConfig(object) {
	$("#tile-configuration").find(".save-changes").click( function () {
    var form = $("#tile-configuration").find("form");
    form.off('submit');
    form.on('submit', $.proxy(function (e) {
			getTileFormValue(form, "tile-configuration", object)
	    $("#tile-configuration").modal('hide');
	    e.preventDefault();
    }, object));
	});
}

function TileFactory() {}
TileFactory.create = function (object) {
	var newDiv = $(handlebars("#tile-template", {
		tileId: object.id
		, title: object.title
	}));
	var row = 0;// row
	var clickedRow;// clicked row
	if(defaultPlusBtn) { // check its new row
		newDiv = createNewRow(newDiv, object)
	} else {// row button action
		var splitValue = customBtn.id.split("-");
		var rowObject = panelRow[splitValue[1] - 1];
		clickedRow = rowObject.id
		if(object.widgetType != rowObject.widgetType) {// f choosen type and row type is not equal
			newDiv = createNewRow(newDiv, object);
			defaultPlusBtn = true;
		}

		if(object.widgetType == 'small'&& rowObject.widgetType == 'small') {
			var findElement = $("."+customBtn.id);
			if(findElement.find(".row-col-1").length == 0) {
				newDiv.addClass('row-col-1');
				newDiv.append("<div><button data-target='#addWidgetModal' class='tile-add-btn tile-add-btn btn btn-primary filter-nav-button glyphicon glyphicon-plus custom-add-btn row-col-1'onClick='setClicketData(this)'  data-toggle='modal' id='row-"+splitValue[1]+"'></button><div>");
			}
		}
	}

	if(object.widgetType == "full") {
		newDiv.find(".tile").addClass('col-md-12');
	} else if(object.widgetType == "medium") {
		newDiv.find(".tile").addClass('col-md-6');
		newDiv.find(".tile").width(590);
	} else if(object.widgetType == "small") {
		newDiv.find(".tile").addClass('col-md-4');
		newDiv.find(".tile").width(300);
		newDiv.find(".tile").height(200);
	}

	newDiv.find(".chart-item").append('<div id="'+object.id+'-health-text" class="lineGraph-health-text">10,000</div>');
	newDiv.find(".chart-item").append('<div id="'+object.id+'-health" style=""></div>');
	newDiv.find(".chart-item").append('<div id="'+object.id+'"></div>');

	if(defaultPlusBtn) {// new row
		newDiv.insertBefore('.float-clear');
	} else {// remove row btn and add new div based on type
		customBtn.remove();
		newDiv.insertAfter('#'+clickedRow);
	}

	if(object.chartType == "line") {
		setInterval(function(){
    //code goes here that will be run every 5 seconds.
			var lineGraph = new LineTile();
			lineGraph.render(newDiv, object);
			}, 5000);
	}

	triggerConfig(newDiv, object);// add event for tile config
	updateTile(object);
	saveTileConfig(object);// add event for tile save btn
};
