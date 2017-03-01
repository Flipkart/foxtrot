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

function setConfigValue(object, index) {
	var form = $("#tile-configuration").find("form");
	var index = tileData.indexOf(object.id);
	form.find(".tile-title").val(tileData[index].title);
	form.find(".tile-table").val(tileData[index].table);
	form.find(".tile-time-unit").val(tileData[index].timeUnit);
	form.find(".tile-time-value").val(tileData[index].timeValue);
	form.find(".tile-chart-type").val(tileData[index].chartType);
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
		panelRow.push({type : object.widgetType, id : object.id});
		newDiv.addClass("row-"+row);
	} else { // incremetn row value by one
		panelRow.push({type : object.widgetType, id : object.id});
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
	$("#tile-configuration").modal('show');
	$("#tile-configuration").find(".tileId").val(object.id);
	var tileListIndex = tileList.indexOf(object.id);
	var tileDataIndex = tileData[tileListIndex];
	if(tileDataIndex != undefined) {
		setConfigValue(object, tileDataIndex);
	} else {
		clearForm();
	}
	});
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

	if(object.widgetType == "full") {
		newDiv.find(".chart-item").append('<div id="'+object.id+'" class="height:500px;"></div>');
	}

	if(defaultPlusBtn) {// new row
		newDiv.insertBefore('.float-clear');
	} else {// remove row btn and add new div based on type
		customBtn.remove();
		newDiv.insertAfter('#'+clickedRow);
	}

	if(object.widgetType == "full") {
		var chartObject = {
        "8": 77,
        "9": 187,
        "lollypop": 123,
        "ics": 58,
        "marshmallow": 176,
        "kitkat": 315,
        "jellybean": 64
    };

		var yValue = [];
		var xValue = [];
		var index = 0;
		for (var key in chartObject) {
			if (chartObject.hasOwnProperty(key)) {
				yValue.push([index, chartObject[key]]);
				xValue.push([index, key])
			}
			index++;
		}

		var chartDiv = newDiv.find(".chart-item");
		var ctx = chartDiv.find("#"+object.id);
		console.log(ctx);
		ctx.width(ctx.width);
		ctx.height(200);
		$.plot(ctx, [
			{ data: yValue },
		], {
			series: {
				lines: { show: true },
				points: { show: false }
			},
			xaxis: {
				ticks: xValue,
				tickLength:0
			},
			grid: {
				hoverable: true,
        color: "#B2B2B2",
        show: true,
        borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
        borderColor: "#EEEEEE",
			},
			tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        }
		});
	}

	triggerConfig(newDiv, object);// add event for tile config
	updateTile(object);
	saveTileConfig(object);// add event for tile save btn
};
