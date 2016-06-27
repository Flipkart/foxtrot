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
    console.log("bar-tile: constructor");
    this.typeName = "bar";
    this.setupModalName = "#setupBarChartModal";
//Instance properties
    this.eventTypeFieldName = null;
    this.selectedValues = null;
    this.period = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uiFilteredValues = [];
    this.doCompare = false;
}

BarTile.prototype = new Tile();

BarTile.prototype.renderWithCompare = function (data, dataPrevious, animate) {
    console.log("bar-tile: renderWithCompare");

    var tileElement = $("#" + this.id);
    var parent = $("#content-for-" + this.id);
    var parentWidth = parent.width();

    if (this.title) {
        $(tileElement).find(".tile-header").text(this.title);
    } else {
        $(tileElement).find(".tile-header").text("Group by " + this.eventTypeFieldName);
    }

    var chartLabel = null;
    if (0 === parent.find(".pielabel").length) {
        chartLabel = $("<div>", {class: "pielabel"});
        parent.append(chartLabel);
    } else {
        chartLabel = parent.find(".pielabel");
    }
    chartLabel.text(getPeriodString(this.period, tileElement.find(".period-select").val()));

    var canvas = null;
    var legendArea = null;
    var legendAreaPrevious = null;
    if (this.showLegend) {
        if (0 === parent.find(".chartcanvas").length) {
            canvasParent = $("<div>", {class: "chartcanvas"});

            canvas = $("<div>", {class: "group-chart-area"});
            canvasParent.append(canvas);

            legendArea = $("<div>", {class: "group-legend-area"});
            canvasParent.append(legendArea);

            legendAreaPrevious = $("<div>", {class: "group-legend-area-previous"});
            canvasParent.append(legendAreaPrevious);

            parent.append(canvasParent);
        } else {
            canvas = parent.find(".chartcanvas").find(".group-chart-area");
            legendArea = parent.find(".chartcanvas").find(".group-legend-area");
            legendAreaPrevious = parent.find(".chartcanvas").find(".group-legend-area-previous");
        }

        var canvasHeight = canvas.height();
        var canvasWidth = canvas.width();
        canvas.width(0.58 * canvas.parent().width());
        legendArea.width((canvas.parent().width() - canvas.width() - 50) / 2 - 30);
        legendAreaPrevious.width((canvas.parent().width() - canvas.width() - 50) / 2 - 30);
        chartLabel.width(canvas.width());
        parentWidth = canvasWidth;
        //chartLabel.height(canvas.height());
    } else {
        if (0 === parent.find(".chartcanvas").length) {
            canvas = $("<div>", {class: "chartcanvas"});
            parent.append(canvas);
        }
        else {
            canvas = parent.find(".chartcanvas");
        }
    }


    if (!data.hasOwnProperty("result") && !dataPrevious.hasOwnProperty("result")) {
        canvas.empty();
        if (this.showLegend) {
            legendArea.empty();
            legendAreaPrevious.empty();
        }
        return;
    }

    var columns = [];
    var previousColumns = [];
    var ticks = [];
    var i = 0;
    this.uniqueValues = [];
    var flatData = [];
    var flatDataPrevious = [];
    var combinedData = [];


    //new code
    var tmpData = new Object();
    if (data.hasOwnProperty("result")) {
        for (property in data.result) {
            if (!tmpData.hasOwnProperty(property)) {
                tmpData[property] = new Object();
            }
            tmpData[property]["curr"] = data.result[property];
        }
    }
    if (dataPrevious.hasOwnProperty("result")) {
        for (property in dataPrevious.result) {
            if (!tmpData.hasOwnProperty(property)) {
                tmpData[property] = new Object();
            }
            tmpData[property]["prev"] = dataPrevious.result[property];
        }
    }
    if (0 == Object.keys(tmpData).length) {
        canvas.empty();
        return;
    }
    //  var propertyWiseData = new Object();
    for (var property in tmpData) {
        this.uniqueValues.push(property);
        var count = 0;
        var propertyData = tmpData[property];
        if (propertyData.hasOwnProperty("curr")) {
            count = propertyData["curr"];
        }
        var dataElement = [i, count];
        if (this.isValueVisible(property)) {
            columns.push(dataElement);
            flatData.push({label: property, data: count, color: "#AA4643", day: "Current"});
            ticks.push([i, property]);
        }
        count = 0;
        if (propertyData.hasOwnProperty("prev")) {
            count = propertyData["prev"];
        }
        dataElement = [i, count];
        if (this.isValueVisible(property)) {
            previousColumns.push(dataElement);
            flatDataPrevious.push({label: property, data: count, color: "#89A54E", day: "Previous"});

        }
        i++;
    }
    combinedData.push({
        label: "Current",
        data: columns,
        bars: {
            show: true,
            label: {
                show: true
            },
            barWidth: 0.2,
            align: "center",
            lineWidth: 1.0,
            fill: true,
            fillColor: "#AA4643",
            order: 2
        },
        color: "#AA4643"
    });
    combinedData.push({
        label: "Previous",
        data: previousColumns,
        bars: {
            show: true,
            label: {
                show: true
            },
            barWidth: 0.2,
            align: "center",
            lineWidth: 1.0,
            fill: true,
            fillColor: "#89A54E",
            order: 1
        },
        color: "#89A54E"
    });
    // }

    var xAxisOptions = {
        tickLength: 0,
        labelWidth: 0,
        axisLabelPadding: 5,
        axisLabelUseCanvas: true
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
            shadowSize: 1,
            valueLabels: {
                show: false
            }
        },
        legend: {
            show: false
        },

        xaxis: xAxisOptions,

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

    $.plot(canvas, combinedData, chartOptions);
    drawLegend(flatData, legendArea);
    drawLegendPrevious(flatDataPrevious, legendAreaPrevious);
};

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
    chartLabel.text(getPeriodString(this.period, tileElement.find(".period-select").val()));

    var canvas = null;
    var legendArea = null;
    if (this.showLegend) {
        if (0 === parent.find(".chartcanvas").length) {
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
        if (0 === parent.find(".chartcanvas").length) {
            canvas = $("<div>", {class: "chartcanvas"});
            parent.append(canvas);
        }
        else {
            canvas = parent.find(".chartcanvas");
        }
    }

    if (!data.hasOwnProperty("result")) {
        canvas.empty();
        if (this.showLegend) {                  // Bug Eliminated
            legendArea.empty();
        }
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
            var dataElement = {label: property, data: [[i, data.result[property]]], color: colors.nextColor()};
            columns.push(dataElement);
            ticks.push([i, property]);
            flatData.push({label: property, data: data.result[property], color: dataElement.color});
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

        xaxis: xAxisOptions,

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

BarTile.prototype.getQuery = function (noOfDaysOld) {
    if (noOfDaysOld === undefined) noOfDaysOld = 0;

    console.log("bar-tile: getQuery");
    if (this.eventTypeFieldName && this.period != 0) {
        var filters = [];

        filters.push(timeValue(this.period, $("#" + this.id).find(".period-select").val(), noOfDaysOld));

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
        return JSON.stringify({
            opcode: "group",
            table: this.tables.selectedTable.name,
            filters: filters,
            nesting: [this.eventTypeFieldName]
        });
    }
};

BarTile.prototype.isSetupDone = function () {
    console.log("bar-tile: isSetupDone");
    return this.eventTypeFieldName && this.period != 0;
};

BarTile.prototype.configChanged = function () {
    console.log("bar-tile: configChanged");
    var modal = $(this.setupModalName);
    this.period = parseInt(modal.find(".refresh-period").val());
    this.eventTypeFieldName = modal.find(".bar-chart-field").val();
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
    this.doCompare = modal.find(".bar-do-compare").prop('checked');
    $("#content-for-" + this.id).find(".chartcanvas").remove();
    $("#content-for-" + this.id).find(".pielabel").remove();
};

BarTile.prototype.populateSetupDialog = function () {
    console.log("bar-tile: populatesetupDialog");
    var modal = $(this.setupModalName);
    modal.find(".tile-title").val(this.title)
    var select = modal.find("#bar-chart-field");
    select.find('option').remove();
    for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
        select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
    }

    if (this.eventTypeFieldName) {
        select.val(this.eventTypeFieldName);
    }
    select.selectpicker('refresh');
    modal.find(".refresh-period").val(( 0 != this.period) ? this.period : "");
    if (this.selectedValues) {
        modal.find(".selected-values").val(this.selectedValues.join(", "));
    }
    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".bar-show-legend").prop('checked', this.showLegend);
    modal.find(".bar-do-compare").prop('checked', this.doCompare);
};

BarTile.prototype.registerSpecificData = function (representation) {
    console.log("bar-tile: registerSpecificData");
    representation['period'] = this.period;
    representation['eventTypeFieldName'] = this.eventTypeFieldName;
    representation['selectedValues'] = this.selectedValues;
    representation['showLegend'] = this.showLegend;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

BarTile.prototype.loadSpecificData = function (representation) {
    console.log("bar-tile: loadSpecificData");
    this.period = representation['period'];
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
    console.log("bar-tile: isValueVisible");
    return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
};

BarTile.prototype.getUniqueValues = function () {
    console.log("bar-tile: getUniqueValues");
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

BarTile.prototype.filterValues = function (values) {
    console.log("bar-tile: filterValues");
    if (!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = new Object();
    for (var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
}

BarTile.prototype.getCompareStatus = function () {
    return this.doCompare;
};