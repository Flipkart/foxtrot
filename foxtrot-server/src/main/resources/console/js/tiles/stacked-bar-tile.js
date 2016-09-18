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

function StackedBar() {
    this.typeName = "stacked_bar";
    this.refresh = true;
    this.setupModalName = "#setupStackedBarChartModal";
    //Instance properties
    this.eventTypeFieldName = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uniqueCountOn = null;
    this.uiFilteredValues;
}

StackedBar.prototype = new Tile();

StackedBar.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Trend for " + this.eventTypeFieldName);
    }

    var parent = $("#content-for-" + this.id);
    var canvas = null;
    if (!parent || 0 == parent.find(".chartcanvas").length) {
        //$("#content-for-" + this.id).append("<div class='chart-content'/>");
        parent = $("#content-for-" + this.id);//.find(".chart-content");
        //parent.append("<div style='height: 15%'><input type='text' class='form-control col-lg-12 eventfilter' placeholder='Start typing here to filter event type...'/></div>");
        canvas = $("<div>", {class: "chartcanvas"});
        parent.append(canvas);
        legendArea = $("<div>", {class: "legendArea"});
        //legendArea.height("10%");
        //legendArea.width("100%");
        parent.append(legendArea);
    }
    else {
        canvas = parent.find(".chartcanvas");
    }
    if (!data.hasOwnProperty("trends")) {
        canvas.empty();
        return;
    }
    var colors = new Colors(Object.keys(data.trends).length);
    var d = [];
    var colorIdx = 0;
    var timestamp = new Date().getTime();
    var tmpData = new Object();
    var filterField = parent.find(".eventfilter").val();
    var regexp = null;
    if (filterField) {
        regexp = new RegExp(filterField, 'i');
    }

    for (var trend in data.trends) {
        if (regexp && !regexp.test(trend)) {
            continue;
        }
        var trendData = data.trends[trend];
        for (var i = 0; i < trendData.length; i++) {
            var time = trendData[i].period;
            var count = trendData[i].count / Math.pow(10, this.ignoreDigits);
            if (!tmpData.hasOwnProperty(time)) {
                tmpData[time] = new Object();
            }
            tmpData[time][trend] = count;
        }
    }
    if (0 == Object.keys(tmpData).length) {
        canvas.empty();
        return;
    }

    var trendWiseData = new Object();
    for (var time in tmpData) {
        for (var trend in data.trends) {
            if (regexp && !regexp.test(trend)) {
                continue;
            }
            var count = 0;
            var timeData = tmpData[time];
            if (timeData.hasOwnProperty(trend)) {
                count = timeData[trend];
            }
            var rows = null;
            if (!trendWiseData.hasOwnProperty(trend)) {
                rows = [];
                trendWiseData[trend] = rows;
            }
            rows = trendWiseData[trend];
            var timeVal = parseInt(time);
            rows.push([timeVal, count]);
        }
    }
    this.uniqueValues = [];
    for (var trend in trendWiseData) {
        var rows = trendWiseData[trend];
        if (regexp && !regexp.test(trend)) {
            continue;
        }
        rows.sort(function (lhs, rhs) {
            return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0]) ? 0 : 1);
        })
        if (this.isValueVisible(trend)) {
            d.push({
                data: rows,
                color: colors[colorIdx],
                label: trend,
                fill: 0.3,
                fillColor: "#A3A3A3",
                lines: {show: true},
                shadowSize: 0/*, curvedLines: {apply: true}*/
            });
        }
        this.uniqueValues.push(trend);
    }
    $.plot(canvas, d, {
        series: {
            stack: true,
            lines: {
                show: true,
                fill: true,
                lineWidth: 1.0,
                fillColor: {colors: [{opacity: 0.7}, {opacity: 0.1}]}
            }/*,
             curvedLines: { active: true }*/
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
            timeformat: axisTimeFormat(this.periodUnit, this.customInterval()),
        },
        selection: {
            mode: "x",
            minSize: 1
        },
        tooltip: true,
        tooltipOpts: {
            content: /*function(label, x, y) {
             var date = new Date(x);
             return label + ": " + y + " at " + date;
             }*/"%s: %y events at %x",
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
    //TODO:: FILL UP LEGEND AREA FIRST AND THEN FILL UP UPPER PART
};

StackedBar.prototype.getQuery = function () {
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
            opcode: "trend",
            table: table,
            filters: filters,
            field: this.eventTypeFieldName,
            uniqueCountOn: this.uniqueCountOn && this.uniqueCountOn != "none" ? this.uniqueCountOn : null,
            period: periodFromWindow(this.periodUnit, this.customInterval())
        });
    }
};

StackedBar.prototype.isSetupDone = function () {
    return this.eventTypeFieldName && this.periodValue != 0 && this.periodUnit;
};

StackedBar.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.eventTypeFieldName = modal.find(".stacked-bar-chart-field").val();
    this.uniqueCountOn = modal.find("#stacked-bar-unique-field").val();


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

StackedBar.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);

    var field_select = modal.find("#stacked-bar-chart-field");
    field_select.find('option').remove();

    var unique_field_select = modal.find("#stacked-bar-unique-field");
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

StackedBar.prototype.populateSetupDialog = function () {
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
    modal.find("#stacked-bar-unique-field").val(this.uniqueCountOn);

    if (this.selectedFilters) {
        modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".ignored-digits").val(this.ignoreDigits);

};

StackedBar.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['eventTypeFieldName'] = this.eventTypeFieldName;
    representation['uniqueCountOn'] = this.uniqueCountOn;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
};

StackedBar.prototype.loadSpecificData = function (representation) {
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
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
};

StackedBar.prototype.isValueVisible = function (value) {
    return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
};

StackedBar.prototype.getUniqueValues = function () {
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

StackedBar.prototype.filterValues = function (values) {
    if (!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = new Object();
    for (var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
};
