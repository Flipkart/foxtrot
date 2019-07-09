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

function TablesView(id, tables) {
	this.id = id;
	this.tables = tables;
	this.tableSelectionChangeHandler = null;
}

TablesView.prototype.load = function(tables) {
	var select = $(this.id);
	select.find('option')
    .remove();
	for (var i = tables.length - 1; i >= 0; i--) {
		select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
	}
	select.val(this.tables.getSelectionIndex());
	select.selectpicker('refresh');
	select.change();
};

TablesView.prototype.registerTableSelectionChangeHandler = function(handler) {
	this.tableSelectionChangeHandler = handler;
};

TablesView.prototype.init = function() {
	this.tables.registerTableChangeHandler($.proxy(this.load, this));
	$(this.id).change($.proxy(function(){
		var value = parseInt($(this.id).val());
		var table = this.tables.tables[value];
		if(table) {
			if(this.tableSelectionChangeHandler) {
				this.tableSelectionChangeHandler(table.name);
			}
			this.tables.loadTableMeta(table);
			this.tables.selectedTable = table;
			console.log("Table changed to: " + table.name);
		}
	}, this));
	this.tables.init();
};

function FoxTrot() {
    this.tables = new Tables();
    this.tablesView = new TablesView("#tables", this.tables);
    this.selectedTable = null;
    this.tableSelectionChangeHandlers = [];
    this.queue = new Queue();
    this.tileSet = new TileSet("#tileContainer", this.tables);
    this.consoleManager = new ConsoleManager(this.tileSet, this.queue, this.tables);
    this.filterSection = new FilterSection(".filter-main", this.tables);

    // This is being done so that text remains selectable within table elements
    $("#tileContainer").sortable({
        revert: true,
        cancel: "#tileContainer,td"
    });

    $("#tileContainer").on("sortstart", function () {
        $(".tile").addClass("tile-drag");
    });

    $("#tileContainer").on("sortstop", function () {
        $(".tile").removeClass("tile-drag");
    });

    $("#saveConsole").click($.proxy(this.consoleManager.saveConsole, this.consoleManager));
}

FoxTrot.prototype.init = function() {
	this.tablesView.registerTableSelectionChangeHandler($.proxy(function(value){
		this.selectedTable = value;
		if(this.tableSelectionChangeHandler && value) {
			for (var i = this.tableSelectionChangeHandlers.length - 1; i >= 0; i--) {
				this.tableSelectionChangeHandlers[i](value);
			};
		}
	}, this));
	this.tablesView.init();
	this.queue.start();
	//this.tablesView.registerTableSelectionChangeHandler($.proxy(this.queue.executeCalls, this.queue));
};

FoxTrot.prototype.registerTableSelectionChangeHandler = function(handler) {
	this.tableSelectionChangeHandlers.push(handler);
};

FoxTrot.prototype.addTile = function() {
	var type = $("#widgetType").val();
	var tile = TileFactory.create(type);
	tile.init(guid(), this.queue, this.tables);
	this.tileSet.register(tile);
	$("#addWidgetModal").modal('hide');
	success("<strong>Tile added!!</strong> Click the settings icon on the widget to setup the tile and see your data..");
	this.queue.executeCalls();
};

FoxTrot.prototype.saveConsole = function(e) {
	var representation = this.consoleManager.getConsoleRepresentation();
	var modal = $("#saveConsoleModal");
	var name = modal.find(".console-name").val();
	representation['id'] = name.trim().toLowerCase().split(' ').join("_");
	representation['updated'] = new Date().getTime();
	representation['name'] = name;

	console.log(JSON.stringify(representation));
	$.ajax({
		url: hostDetails.url("/foxtrot/v1/consoles"),
		type: 'POST',
		contentType: 'application/json',
		data: JSON.stringify(representation),
		success: function() {
			success("Saved console. The new console can be accessed <a href='?console=" + representation.id + "' class='alert-link'>here</a>");
		},
		error: function() {
			error("Could not save console");
		}
	})
	modal.modal('hide');
	e.preventDefault();
};

FoxTrot.prototype.loadConsole = function(consoleId) {
	$.ajax({
		url: hostDetails.url("/foxtrot/v1/consoles/" + consoleId),
		type: 'GET',
		contentType: 'application/json',
		success: $.proxy(this.consoleManager.buildConsoleFromRepresentation, this.consoleManager),
		error: function() {
			error("Could not save console");
		}
	})
};

FoxTrot.prototype.loadConsoleList = function() {
	$.ajax({
		url: hostDetails.url("/foxtrot/v1/consoles/"),
		type: 'GET',
		contentType: 'application/json',
		success: function(data){
			for (var i = data.length - 1; i >= 0; i--) {
				var select = $("#select_console_name");
				select.find('option')
			    .remove();
				for (var i = data.length - 1; i >= 0; i--) {
					select.append("<option value='" + data[i].id + "'>" + data[i].name + '</option>');
				};
				select.val(0);
				select.selectpicker('refresh');
				select.change();
			};
		},
		error: function() {
			error("Could not save console");
		}
	})
};

$(document).ready(function(){
	$(".alert").alert();
    $(".alert").hide();
    	    var data = [
                {
                    id: "percentiles.1.0",
                    label: "1% Value"
                },
                {
                    id: "percentiles.5.0",
                    label: "5% Value"
                },
                {
                    id: "percentiles.25.0",
                    label: "25% Value"
                },
                {
                    id: "percentiles.50.0",
                    label: "50% Value"
                },
                {
                    id: "percentiles.75.0",
                    label: "75% Value"
                },
                {
                    id: "percentiles.95.0",
                 label: "95% Value"
                },
                {
                   id: "percentiles.99.0",
                   label: "99% Value"
                },
                {
                   id: "stats.avg",
                   label: "Average"
                },
                {
                    id: "stats.count",
                    label: "Count"
                 },
                 {
                    id: "stats.max",
                    label: "Max"
                },
                {
                    id: "stats.min",
                    label: "Min"
                },
                {
                    id: "stats.std_deviation",
                    label: "Std Deviation Value"
                },
                {
                    id: "stats.sum",
                    label: "Sum"
                },
                {
                    id: "stats.sum_of_squares",
                    label: "Sum of Squares"
                },
                {
                    id: "stats.variance",
                    label: "Variance"
                }
            ];
            $(".stats_to_plot").multiselect();
            $(".filter_values").multiselect({
                numberDisplayed: 0
            });
            //$(".period-select").multiselect();

	$("#setupPieChartModal").validator();
	$("#setupBarChartModal").validator();
	$("#setupHistogramForm").validator();
	$("#setupEventBrowser").validator();
	$("#setupStackedChartModal").validator();
	$("#setupStackedBarChartModal").validator();
	$("#setupFqlTableModal").validator();
	$("#setupStatsModal").validator();
	$("#setupStatsTrendChartModal").validator();
	$("#saveConsoleModal").validator();
	$("#loadConsoleModal").validator();
	$(".filter-condition-form").validator();
	// $("#histogram_settings_form").bootstrapValidator();
	var foxtrot = new FoxTrot();

	var configSaveform = $("#saveConsoleModal").find("form");
	configSaveform.off('submit');
	configSaveform.on('submit', $.proxy(foxtrot.saveConsole, foxtrot));

	var configLoadform = $("#loadConsoleModal").find("form");
			configLoadform.off('submit');
			configLoadform.on('submit', function(e){
				var console = $("#select_console_name").val();
				if(console) {
			window.location.assign("?console=" + console);
		}
		e.preventDefault();
	});

	$("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
	$("#loadConsoleModal").on('shown.bs.modal', $.proxy(foxtrot.loadConsoleList, foxtrot));
	foxtrot.init();

	//Check if a console is specified.
	//If yes, render the UI accordingly...

	var consoleId = getParameterByName("console").replace('/','');
	if(consoleId) {
		info("Loading console: " + consoleId);
		foxtrot.loadConsole(consoleId);
	}
});

//Initialize libs
$('.selectpicker').selectpicker();
