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

function DonutTile() {
    this.typeName = "donut";
    this.refresh = true;
    this.setupModalName = "#setupPieChartModal";
    //Instance properties
    this.eventTypeFieldName = null;
    this.selectedValues = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uniqueCountOn = null;
    this.uiFilteredValues;
    this.showLegend = false;
}

DonutTile.prototype = new Tile();

DonutTile.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Group by " + this.eventTypeFieldName);
    }
    var parent = $("#content-for-" + this.id);

    var chartLabel = null;
    if (0 == parent.find(".pielabel").length) {
        chartLabel = $("<div>", {class: "pielabel"});
        parent.append(chartLabel);
    }
    else {
        chartLabel = parent.find(".pielabel");
    }
    chartLabel.text(getPeriodString(this.periodUnit, this.periodValue, this.customInterval()));

    var canvas = null;
    var legendArea = null;
    if (this.showLegend) {
        if (0 == parent.find(".chartcanvas").length) {
            canvasParent = $("<div>", {class: "chartcanvas"});
            canvas = $("<div>", {class: "group-chart-area"})
            canvasParent.append(canvas)
            legendArea = $("<div>", {class: "group-legend-area"})
            canvasParent.append(legendArea)
            parent.append(canvasParent);
        }
        else {
            canvas = parent.find(".chartcanvas").find(".group-chart-area");
            legendArea = parent.find(".chartcanvas").find(".group-legend-area");
        }
        var canvasHeight = canvas.height();
        canvas.width(canvasHeight);
        legendArea.width(canvas.parent().width() - canvas.width() - 50);
        chartLabel.width(canvas.width());
        chartLabel.height(canvas.height());
    }
    else {
        if (0 == parent.find(".chartcanvas").length) {
            canvas = $("<div>", {class: "chartcanvas"});
            parent.append(canvas);
        }
        else {
            canvas = parent.find(".chartcanvas");
        }
    }

    if (!data.hasOwnProperty("result")) {
        canvas.empty();
        return;
    }

    var colors = new Colors(Object.keys(data.result).length);
    var columns = [];
    this.uniqueValues = [];
    for (property in data.result) {
        if (this.isValueVisible(property)) {
            columns.push({
                label: property,
                data: data.result[property] / Math.pow(10, this.ignoreDigits),
                color: colors.nextColor(),
                lines: {show: true},
                shadowSize: 0
            });
        }
        this.uniqueValues.push(property);
    }
    var chartOptions = {
        series: {
            pie: {
                innerRadius: 0.8,
                show: true,
                label: {
                    show: false
                }
            }
        },
        legend: {
            show: false
        },
        grid: {
            hoverable: true
        },
        tooltip: true,
        tooltipOpts: {
            content: function (label, x, y) {
                return label + ": " + y;
            }
        }
    };
    $.plot(canvas, columns, chartOptions);
    drawLegend(columns, legendArea);
};

DonutTile.prototype.getQuery = function () {
    if (this.eventTypeFieldName && this.period != 0) {
        var timestamp = new Date().getTime();
        var filters = [];
        filters.push(timeValue(this.periodUnit, this.periodValue, this.customInterval()));
        if (this.selectedValues) {
            filters.push({
                field: this.eventTypeFieldName,
                operator: "in",
                values: this.selectedValues
            });
        }
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
            opcode: "group",
            table: table,
            filters: filters,
            uniqueCountOn: this.uniqueCountOn && this.uniqueCountOn != "none" ? this.uniqueCountOn : null,
            nesting: [this.eventTypeFieldName]
        });
    }
};

DonutTile.prototype.isSetupDone = function () {
    return this.eventTypeFieldName && this.periodValue != 0 && this.periodUnit;
};

DonutTile.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.eventTypeFieldName = modal.find(".pie-chart-field").val();
    this.uniqueCountOn = modal.find("#donut-unique-field").val();
    var values = modal.find(".selected-values").val();
    if (values) {
        this.selectedValues = values.replace(/ /g, "").split(",");
    }
    else {
        this.selectedValues = null;
    }
    var filters = modal.find(".selected-filters").val();
    if (filters != undefined && filters != "") {
        var selectedFilters = JSON.parse(filters);
        if (selectedFilters != undefined) {
            this.selectedFilters = selectedFilters;
        }
    } else {
        this.selectedFilters = null;
    }
    this.showLegend = modal.find(".pie-show-legend").prop('checked');
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val())

    $("#content-for-" + this.id).find(".chartcanvas").remove();
    $("#content-for-" + this.id).find(".pielabel").remove();
};


DonutTile.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);

    var field_select = modal.find("#pie_field");
    field_select.find('option').remove();

    var unique_field_select = modal.find("#donut-unique-field");
    unique_field_select.find('option').remove();
    unique_field_select.append('<option value="none">None</option>');

    this.tables.loadTableMeta(selected_table, function () {
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
            unique_field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.eventTypeFieldName) {
            field_select.val(this.eventTypeFieldName);
        }
        field_select.selectpicker('refresh');

        if (this.uniqueCountOn) {
            unique_field_select.val(this.uniqueCountOn);
        } else {
            unique_field_select.val("none");
        }
        unique_field_select.selectpicker('refresh');
    }.bind(this));
};

DonutTile.prototype.populateSetupDialog = function () {
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
    modal.find("#donut-unique-field").val(this.uniqueCountOn);

    if (this.selectedValues) {
        modal.find(".selected-values").val(this.selectedValues.join(", "));
    }
    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".pie-show-legend").prop('checked', this.showLegend);
    modal.find(".ignored-digits").val(this.ignoreDigits);

}

DonutTile.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['eventTypeFieldName'] = this.eventTypeFieldName;
    representation['selectedValues'] = this.selectedValues;
    representation['showLegend'] = this.showLegend;
    representation['uniqueCountOn'] = this.uniqueCountOn;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }

};

DonutTile.prototype.loadSpecificData = function (representation) {
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

    this.eventTypeFieldName = representation['eventTypeFieldName'];
    this.selectedValues = representation['selectedValues'];
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
    if (representation.hasOwnProperty('showLegend')) {
        this.showLegend = representation['showLegend'];
    }
};

DonutTile.prototype.isValueVisible = function (value) {
    return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
}

DonutTile.prototype.getUniqueValues = function () {
    var options = [];
    for (var i = 0; i < this.uniqueValues.length; i++) {
        var value = this.uniqueValues[i];
        options.push(
            {
                label: value,
                title: value,
                value: value,
                selected: this.isValueVisible(value)
            }
        );
    }
    return options;
}

DonutTile.prototype.filterValues = function (values) {
    if (!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = new Object();
    for (var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
}