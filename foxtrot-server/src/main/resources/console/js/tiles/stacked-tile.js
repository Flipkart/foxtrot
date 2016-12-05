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

function Stacked() {
    this.typeName = "stacked";
    this.refresh = true;
    this.setupModalName = "#setupStackedChartModal";
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    //Instance properties

    this.stackingKey = null;
    this.groupingKey = null;
    this.uniqueCountOn = null;
}

Stacked.prototype = new Tile();

Stacked.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    }

    var parent = $("#content-for-" + this.id);
    var canvas = null;
    if (!parent || 0 == parent.find(".chartcanvas").length) {
        parent = $("#content-for-" + this.id);//.find(".chart-content");
        canvas = $("<div>", {class: "chartcanvas"});
        parent.append(canvas);
        legendArea = $("<div>", {class: "legendArea"});
        parent.append(legendArea);
    }
    else {
        canvas = parent.find(".chartcanvas");
    }
    if (!data.hasOwnProperty("result")) {
        canvas.empty();
        return;
    }

    var queryResult = data.result;

    // First Get unique x-axis values and define x-axis index for them
    var xAxisTicks = [];
    var xAxisTicksMap = {};
    var index = 0;
    for (var xAxisKey in queryResult) {
        if (!queryResult.hasOwnProperty(xAxisKey)) {
            continue;
        }
        xAxisTicks.push([index, xAxisKey]);
        xAxisTicksMap[xAxisKey] = index;
        index += 1;
    }

    // Now calculate all possible y axis values
    var yAxisTicks = {};
    var yAxisSeriesMap = {};
    index = 0;
    for (xAxisKey in queryResult) {
        if (!queryResult.hasOwnProperty(xAxisKey)) {
            continue;
        }

        for (var yAxisKey in queryResult[xAxisKey]) {
            if (!queryResult[xAxisKey].hasOwnProperty(yAxisKey)) {
                continue;
            }
            if (!yAxisTicks.hasOwnProperty(yAxisKey)) {
                yAxisTicks[yAxisKey] = index;
                yAxisSeriesMap[yAxisKey] = [];
                index += 1;
            }
        }
    }


    // Now define y-axis series data
    for (xAxisKey in queryResult) {
        if (!queryResult.hasOwnProperty(xAxisKey)) {
            continue;
        }
        var xAxisKeyData = queryResult[xAxisKey];
        for (yAxisKey in yAxisSeriesMap) {
            if (!yAxisSeriesMap.hasOwnProperty(yAxisKey)) {
                continue;
            }

            if (xAxisKeyData.hasOwnProperty(yAxisKey)) {
                yAxisSeriesMap[yAxisKey].push([xAxisTicksMap[xAxisKey], xAxisKeyData[yAxisKey]])
            } else {
                yAxisSeriesMap[yAxisKey].push([xAxisTicksMap[xAxisKey], 0])
            }


        }
    }
    var yAxisSeries = [];
    for (var yAxisSeriesElement in yAxisSeriesMap) {
        if (!yAxisSeriesMap.hasOwnProperty(yAxisSeriesElement)) {
            continue;
        }
        if (yAxisSeriesMap[yAxisSeriesElement].length > 0) {
            yAxisSeries.push({label: yAxisSeriesElement, data: yAxisSeriesMap[yAxisSeriesElement]})
        }
    }
    $.plot(canvas, yAxisSeries, {
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
            stack: true
        },
        grid: {
            hoverable: true,
            color: "#B2B2B2",
            show: true,
            borderWidth: 1,
            borderColor: "#EEEEEE"
        },
        xaxis: {
            ticks: xAxisTicks
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: "%s: %y events at %x",
            defaultFormat: true
        },
        legend: {
            show: true,
            noColumns: yAxisSeries.length,
            labelFormatter: function (label, series) {
                return '<font color="black"> &nbsp;' + label + ' &nbsp;</font>';
            },
            container: parent.find(".legendArea")
        }
    });
};

Stacked.prototype.getQuery = function () {
    if (this.isSetupDone()) {
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
            opcode: "group",
            table: table,
            filters: filters,
            nesting: [this.groupingKey, this.stackingKey],
            uniqueCountOn: this.uniqueCountOn && this.uniqueCountOn != "none" ? this.uniqueCountOn : null,
        });
    }
};

Stacked.prototype.isSetupDone = function () {
    return this.stackingKey && this.groupingKey && this.periodValue != 0 && this.periodUnit;
};

Stacked.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.stackingKey = modal.find(".stacked-stacking-key").first().val();
    this.groupingKey = modal.find(".stacked-grouping-key").first().val();
    this.uniqueCountOn = modal.find("#stacked-unique-field").val();

    var filters = modal.find(".selected-filters").val();
    if (filters != undefined && filters != "") {
        var selectedFilters = JSON.parse(filters);
        if (selectedFilters != undefined) {
            this.selectedFilters = selectedFilters;
        }
    } else {
        this.selectedFilters = null;
    }
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val());

};

Stacked.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);
    var stacking_select = modal.find(".stacked-stacking-key").first();
    stacking_select.find('.option').remove();
    this.tables.loadTableMeta(selected_table, function () {
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            stacking_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.stackingKey) {
            stacking_select.val(this.stackingKey);
        }
        stacking_select.selectpicker('refresh');
    }.bind(this));


    var grouping_select = modal.find(".stacked-grouping-key").first();
    grouping_select.find('.option').remove();
    this.tables.loadTableMeta(selected_table, function () {
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            grouping_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.groupingKey) {
            grouping_select.val(this.groupingKey);
        }
        grouping_select.selectpicker('refresh');
    }.bind(this));

    var unique_field_select = modal.find("#stacked-unique-field");
    unique_field_select.find('option').remove();
    unique_field_select.append('<option value="none">None</option>');

    this.tables.loadTableMeta(selected_table, function () {
        console.log("callback function called");
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            unique_field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.uniqueCountOn) {
            unique_field_select.val(this.uniqueCountOn);
        } else {
            unique_field_select.val("none");
        }
        unique_field_select.selectpicker('refresh');
    }.bind(this));
};

Stacked.prototype.populateSetupDialog = function () {
    var modal = $(this.setupModalName);
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }

    modal.find(".tile-title").val(this.title);
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

    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".ignored-digits").val(this.ignoreDigits);

};

Stacked.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['stackingKey'] = this.stackingKey;
    representation['groupingKey'] = this.groupingKey;
    representation['uniqueCountOn'] = this.uniqueCountOn;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

Stacked.prototype.loadSpecificData = function (representation) {
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

    this.groupingKey = representation['groupingKey'];
    this.stackingKey = representation['stackingKey'];
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }

};
