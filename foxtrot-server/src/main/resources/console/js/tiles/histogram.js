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

function Histogram() {
    this.typeName = "histogram";
    this.setupModalName = "#setupHistogram";
    //Instance properties
    this.selectedFilters = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.uniqueCountOn = null;
}

Histogram.prototype = new Tile();

Histogram.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Event rate for " + this.table + " table");
    }

    var parent = $("#content-for-" + this.id);
    var canvas = null;
    if (!parent || 0 == parent.find(".chartcanvas").length) {
        parent = $("#content-for-" + this.id);
        canvas = $("<div>", {class: "chartcanvas"});
        parent.append(canvas);
        legendArea = $("<div>", {class: "legendArea"});
        parent.append(legendArea);
    }
    else {
        canvas = parent.find(".chartcanvas");
    }
    var times = [];
    if (!data.hasOwnProperty('counts')) {
        chartContent.empty();
        return;
    }
    var rows = [];
    rows.push(['date', 'count']);
    for (var i = data.counts.length - 1; i >= 0; i--) {
        rows.push([data.counts[i].period, (data.counts[i].count / Math.pow(10, this.ignoreDigits))]);
    }

    var timestamp = new Date().getTime();
    var d = {data: rows, color: "#57889C", shadowSize: 0};
    $.plot(canvas, [d], {
        series: {
            lines: {
                show: true,
                lineWidth: 1.0,
                shadowSize: 0,
                fill: true,
                fillColor: {colors: [{opacity: 0.7}, {opacity: 0.1}]}
            }
        },
        grid: {
            hoverable: true,
            color: "#B2B2B2",
            show: true,
            borderWidth: 1,
            borderColor: "#EEEEEE"
        },
        xaxis: {
            mode: "time",
            timeformat: axisTimeFormat(this.periodUnit, this.customInterval()),
            timezone: "browser"
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        }
    });
};

Histogram.prototype.isSetupDone = function () {
    return this.periodValue != 0 && this.periodUnit;
};

Histogram.prototype.getQuery = function () {
    if (this.isSetupDone()) {
        var timestamp = new Date().getTime();
        var filters = [];
        filters.push(timeValue(this.periodUnit, this.periodValue, this.customInterval()));
        if (this.selectedFilters && this.selectedFilters.filters) {
            for (var i = 0; i < this.selectedFilters.filters.length; i++) {
                filters.push(this.selectedFilters.filters[i]);
            }
        }
        var table = this.table;
        if (!table) {
            table = this.tables.selectedTable.name;
        }
        return JSON.stringify({
            opcode: "histogram",
            table: table,
            filters: filters,
            field: "_timestamp",
            uniqueCountOn: this.uniqueCountOn && this.uniqueCountOn != "none" ? this.uniqueCountOn : null,
            period: periodFromWindow(this.periodUnit, this.customInterval())
        });
    }
};

Histogram.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.uniqueCountOn = modal.find("#histogram-unique-field").val();

    var filters = modal.find(".selected-filters").val();
    if (filters != undefined && filters != "") {
        var selectedFilters = JSON.parse(filters);
        if (selectedFilters != undefined) {
            this.selectedFilters = selectedFilters;
        }
    } else {
        this.selectedFilters = null;
    }
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val())
    console.log("Config changed for: " + this.id);
};

Histogram.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);
    var field_select = modal.find("#histogram-unique-field");
    field_select.find('option').remove();

    this.tables.loadTableMeta(selected_table, function () {
        field_select.append('<option value="none">None</option>');
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }
        if (this.uniqueCountOn) {
            field_select.val(this.uniqueCountOn);
        } else {
            field_select.val("none");
        }
        field_select.selectpicker('refresh');
    }.bind(this));
};


Histogram.prototype.populateSetupDialog = function () {
    var modal = $(this.setupModalName);
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }

    modal.find(".tile-title").val(this.title);

    // Create list of tables
    this.loadTableList();

    // Setup list of initial fields available
    var selected_table = extractSelectedTable(this.table, this.tables.tables);
    this.tables.loadTableMeta(selected_table, this.loadFieldList.bind(this));

    // Now attach listener for change event so that changing table name changes field list as well
    var selected_table_tag = modal.find(".tile-table").first();
    selected_table_tag.on("change", this.loadFieldList.bind(this));


    modal.find(".tile-time-unit").first().val(this.periodUnit);
    modal.find(".tile-time-unit").first().selectpicker("refresh");
    modal.find(".tile-time-value").first().val(this.periodValue);
    modal.find("#histogram-unique-field").val(this.uniqueCountOn);

    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".ignored-digits").val(this.ignoreDigits);
};

Histogram.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['uniqueCountOn'] = this.uniqueCountOn;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

Histogram.prototype.loadSpecificData = function (representation) {
    this.uniqueCountOn = representation['uniqueCountOn'];
    this.periodUnit = representation['periodUnit'];
    if (!this.periodUnit) {
        this.periodUnit = "minutes";
    }
    if (representation['period']) {
        this.periodValue = representation['period'];
    } else {
        this.periodValue = representation['periodValue'];
    }

    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
};

Histogram.prototype.registerComplete = function () {
    $("#" + this.id).find(".glyphicon-filter").hide();
};