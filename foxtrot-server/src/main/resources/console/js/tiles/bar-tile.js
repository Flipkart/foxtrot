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

function BarTile() {
    this.typeName = "bar";
    this.setupModalName = "#setupBarChartModal";
    //Instance properties
    this.eventTypeFieldName = null;
    this.selectedValues = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uniqueCountOn = null;
    this.uiFilteredValues;
}

BarTile.prototype = new Tile();

BarTile.prototype.render = function (data, animate) {
    var tileElement = $("#" + this.id);
    var parent = $("#content-for-" + this.id);
    var parentWidth = parent.width();

    if (this.title) {
        $(tileElement).find(".tile-header").text(this.title);
    } else {
        $(tileElement).find(".tile-header").text("Group by " + this.eventTypeFieldName);
    }

    var chartLabel = null;
    if (0 == parent.find(".pielabel").length) {
        chartLabel = $("<div>", {class: "pielabel"});
        parent.append(chartLabel);
    } else {
        chartLabel = parent.find(".pielabel");
    }
    chartLabel.text(getPeriodString(this.periodUnit, this.periodValue, this.customInterval()));

    var canvas = null;
    var legendArea = null;
    if (this.showLegend) {
        if (0 == parent.find(".chartcanvas").length) {
            canvasParent = $("<div>", {class: "chartcanvas"});
            canvas = $("<div>", {class: "group-chart-area"});
            canvasParent.append(canvas);
            legendArea = $("<div>", {class: "group-legend-area"});
            canvasParent.append(legendArea);
            parent.append(canvasParent);
        } else {
            canvas = parent.find(".chartcanvas").find(".group-chart-area");
            legendArea = parent.find(".chartcanvas").find(".group-legend-area");
        }
        var canvasHeight = canvas.height();
        var canvasWidth = canvas.width();
        canvas.width(0.58 * canvas.parent().width());
        legendArea.width(canvas.parent().width() - canvas.width() - 50);
        chartLabel.width(canvas.width());
        parentWidth = canvasWidth;
        //chartLabel.height(canvas.height());
    } else {
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
    var ticks = [];
    var i = 0;
    this.uniqueValues = [];
    var flatData = [];
    for (property in data.result) {
        if (this.isValueVisible(property)) {
            var value = data.result[property] / Math.pow(10, this.ignoreDigits);
            var dataElement = {label: property, data: [[i, value]], color: colors.nextColor()};
            columns.push(dataElement);
            ticks.push([i, property]);
            flatData.push({label: property, data: value, color: dataElement.color});
        }
        this.uniqueValues.push(property);
        i++;
    }
    var xAxisOptions = {
        tickLength: 0,
        labelWidth: 0
    };
    var tmpLabel = "";
    for (var i = 0; i < ticks.length; i++) {
        tmpLabel += (ticks[i][1] + " ");
    }
    if (tmpLabel.visualLength() <= parentWidth) {
        xAxisOptions['ticks'] = ticks;
        xAxisOptions['tickFormatter'] = null;
    }
    else {
        xAxisOptions['ticks'] = null;
        xAxisOptions['tickFormatter'] = function () {
            return "";
        }
    }
    var chartOptions = {
        series: {
            bars: {
                show: true,
                label: {
                    show: true
                },
                barWidth: 0.5,
                align: "center",
                lineWidth: 1.0,
                fill: true,
                fillColor: {colors: [{opacity: 0.3}, {opacity: 0.7}]}
            },
            valueLabels: {
                show: true
            }
        },
        legend: {
            show: false
        },
        xaxis: xAxisOptions/*,
         yaxis: {
         tickLength: 1,

         }*/,
        /*grid: {
         hoverable: true,
         borderWidth: {top: 0, right: 0, bottom: 1, left: 1},
         },*/
        grid: {
            hoverable: true,
            color: "#B2B2B2",
            show: true,
            borderWidth: 1,
            borderColor: "#EEEEEE"
        },
        tooltip: true,
        tooltipOpts: {
            content: function (label, x, y) {
                return label + ": " + y;
            }
        }
    };
    $.plot(canvas, columns, chartOptions);
    drawLegend(flatData, legendArea);
};

BarTile.prototype.getQuery = function () {
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

BarTile.prototype.isSetupDone = function () {
    return this.eventTypeFieldName && this.periodValue != 0 && this.periodUnit;
};

BarTile.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();

    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.eventTypeFieldName = modal.find(".bar-chart-field").val();
    this.uniqueCountOn = modal.find("#bar-unique-field").val();
    this.title = modal.find(".tile-title").val();
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
    this.showLegend = modal.find(".bar-show-legend").prop('checked');
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val());
    $("#content-for-" + this.id).find(".chartcanvas").remove();
    $("#content-for-" + this.id).find(".pielabel").remove();
};

BarTile.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);

    var field_select = modal.find("#bar-chart-field");
    field_select.find('option').remove();

    var unique_field_select = modal.find("#bar-unique-field");
    unique_field_select.find('option').remove();
    unique_field_select.append('<option value="none">None</option>');

    this.tables.loadTableMeta(selected_table, function () {
        console.log("callback function called");
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

BarTile.prototype.populateSetupDialog = function () {
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
    modal.find("#bar-unique-field").val(this.uniqueCountOn);

    if (this.selectedValues) {
        modal.find(".selected-values").val(this.selectedValues.join(", "));
    }
    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".bar-show-legend").prop('checked', this.showLegend);
    modal.find(".ignored-digits").val(this.ignoreDigits);
};

BarTile.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['uniqueCountOn'] = this.uniqueCountOn;
    representation['eventTypeFieldName'] = this.eventTypeFieldName;
    representation['selectedValues'] = this.selectedValues;
    representation['showLegend'] = this.showLegend;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

BarTile.prototype.loadSpecificData = function (representation) {
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

BarTile.prototype.isValueVisible = function (value) {
    return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
};

BarTile.prototype.getUniqueValues = function () {
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
};

BarTile.prototype.filterValues = function (values) {
    if (!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = {};
    for (var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
};