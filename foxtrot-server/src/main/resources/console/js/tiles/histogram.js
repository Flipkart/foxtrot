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
    this.period = 0;
    this.doCompare = false;
}

Histogram.prototype = new Tile();

Histogram.prototype.render = function (data, animate) {
    if (this.period == 0) {
        return;
    }
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Event rate for " + this.tables.selectedTable.name + " table");
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
        canvas.empty();
        return;
    }

    legendArea.empty();
    var rows = [];
    rows.push(['date', 'count']);
    for (var i = data.counts.length - 1; i >= 0; i--) {
        rows.push([data.counts[i].period, data.counts[i].count]);
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
        },
        legend: {
            show: false
        }
    });
};

Histogram.prototype.renderWithCompare = function (data, dataPrevious, animate) {
    if (this.period == 0) {
        return;
    }
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Event rate for " + this.tables.selectedTable.name + " table");
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

    if (!data.hasOwnProperty('counts') && !dataPrevious.hasOwnProperty('counts')) {
        canvas.empty();
        legendArea.empty();
        return;
    }

    var d = [];
    if (data.hasOwnProperty('counts')) {
        var rows = [];
        //rows.push(['date', 'count']);
        for (var i = data.counts.length - 1; i >= 0; i--) {
            rows.push([data.counts[i].period, data.counts[i].count]);
        }
        rows.sort(function (lhs, rhs) {
            return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0]) ? 0 : 1);
        })
        d.push({label: "Current", data: rows, color: "#57889C"});
    }

    if (dataPrevious.hasOwnProperty('counts')) {
        var rowsPrevious = [];
        //rowsPrevious.push(['date', 'count']);
        for (var i = dataPrevious.counts.length - 1; i >= 0; i--) {
            rowsPrevious.push([dataPrevious.counts[i].period + (24 * 3600 * 1000), dataPrevious.counts[i].count]);
        }
        rowsPrevious.sort(function (lhs, rhs) {
            return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0]) ? 0 : 1);
        })
        d.push({label: "Previous", data: rowsPrevious, color: "#9c5766"});
    }

    $.plot(canvas, d, {
        series: {
            lines: {
                show: true,
                lineWidth: 1.0,
                shadowSize: 0,
                fill: true,
                fillColor: {colors: [{opacity: 0.7}, {opacity: 0.1}]}
            }/*,
            stack: true*/
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
            timezone: "browser"
        }/*,
        selection: {
            mode: "x",
            minSize: 1
        }*/,
        tooltip: true,
        tooltipOpts: {
            content: "%y events at %x",
            defaultFormat: true
        },
        legend: {
            show: true,
            noColumns: d.length,
            labelFormatter: function (label, series) {
                return '<font color="black"> &nbsp;' + label + ' &nbsp;</font>';
            },
            container: parent.find(".legendArea")
        }
    });
}

Histogram.prototype.isSetupDone = function () {
    return this.period != 0;
};

Histogram.prototype.getQuery = function (noOfDaysOld) {
    if (noOfDaysOld === undefined) noOfDaysOld = 0;

    if (this.period != 0) {
        var timestamp = new Date().getTime();
        var filters = [];

        var psDropDown = $("#" + this.id).find(".period-select").val();
        if (psDropDown==="2d" || psDropDown==="5d" || psDropDown==="7d" || psDropDown==="15d") {
            this.doCompare = false;
        }

        filters.push(timeValue(this.period, psDropDown, noOfDaysOld));
        if (this.selectedFilters && this.selectedFilters.filters) {
            for (var i = 0; i < this.selectedFilters.filters.length; i++) {
                filters.push(this.selectedFilters.filters[i]);
            }
        }
        return JSON.stringify({
            opcode: "histogram",
            table: this.tables.selectedTable.name,
            filters: filters,
            field: "_timestamp",
            period: periodFromWindow($("#" + this.id).find(".period-select").val())
        });
    }
};

Histogram.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.period = parseInt(modal.find(".refresh-period").val());
    this.title = modal.find(".tile-title").val();
    var filters = modal.find(".selected-filters").val();
    if (filters != undefined && filters != "") {
        var selectedFilters = JSON.parse(filters);
        if (selectedFilters != undefined) {
            this.selectedFilters = selectedFilters;
        }
    } else {
        this.selectedFilters = null;
    }
    this.doCompare = modal.find(".hist-do-compare").prop('checked');
    console.log("Config changed for: " + this.id);
};

Histogram.prototype.populateSetupDialog = function () {
    var modal = $(this.setupModalName);
    modal.find(".refresh-period").val(( 0 != this.period) ? this.period : "");
    modal.find(".tile-title").val(this.title)
    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".hist-do-compare").prop('checked', this.doCompare);
}

Histogram.prototype.registerSpecificData = function (representation) {
    representation['period'] = this.period;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

Histogram.prototype.loadSpecificData = function (representation) {
    this.period = representation['period'];
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
};

Histogram.prototype.registerComplete = function () {
    $("#" + this.id).find(".glyphicon-filter").hide();
};

Histogram.prototype.getCompareStatus = function () {
    return this.doCompare;
};