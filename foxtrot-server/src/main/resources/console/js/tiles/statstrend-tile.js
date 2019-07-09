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

function StatsTrend() {
    this.typeName = "statstrend";
    this.refresh = true;
    this.setupModalName = "#setupStatsTrendChartModal";
    //Instance properties
    this.eventTypeFieldName = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    this.selectedStats = [];

}

StatsTrend.prototype = new Tile();

StatsTrend.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Stats for " + this.eventTypeFieldName);
    }

    var parent = $("#content-for-" + this.id);
    var canvas = null;
    if (!parent || 0 == parent.find(".chartcanvas").length) {
        parent = $("#content-for-" + this.id);//.find(".chart-content");
        canvas = $("<div>", {class: "chartcanvas"});
        parent.append(canvas);
        legendArea = $("<div>", {class: "legendArea"});
        parent.append(legendArea)
    }
    else {
        canvas = parent.find(".chartcanvas");
    }

    if (!data.hasOwnProperty("result")) {
        canvas.empty();
        return;
    }

    var results = data.result;
    var selectedStats = this.selectedStats;
    var colors = new Colors(selectedStats.length);
    var d = [];
    var colorIdx = 0;
    for (var j = 0; j < selectedStats.length; j++) {
        d.push({
            data: [],
            color: colors[colorIdx],
            label: selectedStats[j],
            lines: {show: true},
            shadowSize: 0/*, curvedLines: {apply: true}*/
        });
    }
    var colorIdx = 0;
    var timestamp = new Date().getTime();
    var tmpData = new Object();
    for (var i = 0; i < results.length; i++) {
        var stats = results[i].stats;
        var percentiles = results[i].percentiles;
        for (var j = 0; j < selectedStats.length; j++) {
            var selected = selectedStats[j];
            var value = 0;
            if (selected.startsWith('percentiles.')) {
                value = percentiles[selected.split("percentiles.")[1]];
            }
            if (selected.startsWith('stats.')) {
                value = stats[selected.split("stats.")[1]];
            }
            d[j].data.push([results[i].period, value / Math.pow(10, this.ignoreDigits)]);
        }
    }

    $.plot(canvas, d, {
        series: {
            //stack: true,
            lines: {
                show: true,
                fill: 0,
                lineWidth: 2.0,
                fillColor: {colors: [{opacity: 0.7}, {opacity: 0.1}]}
            }/*,
             curvedLines: {  active: true }*/
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
            timezone: "browser",
            timeformat: axisTimeFormat(this.periodUnit, this.customInterval())
        },
        yaxis: {
            tickFormatter: function(val, axis) {
                return numberWithCommas(val);
            }
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: function (label, x, y) {
                var date = new Date(x);
                return label + ": " + numberWithCommas(y.toFixed(2)) + " at " + date.getHours() + ":" + date.getMinutes();
            },
            defaultFormat: true
        },
        legend: {
            show: true,
            position: 'e',
            noColumns: 8,
            noRows: 0,
            labelFormatter: function (label, series) {
                return '<font color="black"> &nbsp;' + label + ' &nbsp;</font>';
            },
            container: parent.find(".legendArea")
        }
    });
};

StatsTrend.prototype.getQuery = function () {
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
            opcode: "statstrend",
            table: table,
            filters: filters,
            field: this.eventTypeFieldName,
            period: periodFromWindow(this.periodUnit, this.customInterval())
        });
    }
};

StatsTrend.prototype.isSetupDone = function () {
    return this.eventTypeFieldName && this.periodValue != 0 && this.periodUnit;
};

StatsTrend.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.eventTypeFieldName = modal.find(".statstrend-bar-chart-field").val();
    var filters = modal.find(".selected-filters").val();
    if (filters != undefined && filters != "") {
        var selectedFilters = JSON.parse(filters);
        if (selectedFilters != undefined) {
            this.selectedFilters = selectedFilters;
        }
    }
    else {
        this.selectedFilters = null;
    }
    this.selectedStats = modal.find(".stats_to_plot").val();
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val());
};


StatsTrend.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);
    var field_select = modal.find("#statstrend-bar-chart-field");
    field_select.find('option').remove();

    this.tables.loadTableMeta(selected_table, function () {
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.eventTypeFieldName) {
            field_select.val(this.eventTypeFieldName);
        }
        field_select.selectpicker('refresh');
    }.bind(this));
};


StatsTrend.prototype.populateSetupDialog = function () {
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

    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find('.stats_to_plot').multiselect('select', this.selectedStats);
    modal.find(".ignored-digits").val(this.ignoreDigits);
};

StatsTrend.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['eventTypeFieldName'] = this.eventTypeFieldName;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
    representation['selectedStats'] = this.selectedStats;
};

StatsTrend.prototype.loadSpecificData = function (representation) {
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
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
    this.selectedStats = representation['selectedStats'];
};