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

function Stats() {
    this.typeName = "stats";
    this.refresh = true;
    this.setupModalName = "#setupStatsModal";
    //Instance properties
    this.statsFieldName = null;
    this.periodUnit = "minutes";
    this.periodValue = 0;
    this.selectedFilters = null;
    this.selectedStat = null;
}

Stats.prototype = new Tile();

Stats.prototype.render = function (data, animate) {
    if (this.title) {
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text(this.selectedStat + " for " + this.statsFieldName);
    }

    var parent = $("#content-for-" + this.id);

    if (0 != parent.find(".dataview-table").length) {
        parent.find(".dataview-table").remove()
    }

    if (!data.hasOwnProperty("result")) {
        return;
    }
    var result = data.result;
    var selected = this.selectedStat;

    var value = 0;
    if (selected.startsWith('percentiles.')) {
        value = result.percentiles[selected.split("percentiles.")[1]].toFixed(2);
    }
    if (selected.startsWith('stats.')) {
        value = result.stats[selected.split("stats.")[1]].toFixed(2);
    }

    value = value / Math.pow(10, this.ignoreDigits);
    value = numberWithCommas(value);
    var chartLabel = null;
    if (0 == parent.find(".statslabel").length) {
        chartLabel = $("<div>", {class: "statslabel"});
        parent.append(chartLabel);
    }
    else {
        chartLabel = parent.find(".statslabel");
    }
    //value = value.replace(/\.00/g, "");
    chartLabel.text(value);

    var headers = [selected];
    var rows = [[value]];
    var tableData = {headers: headers, data: rows};
    //parent.append(handlebars('#table-template', tableData))
};

Stats.prototype.getQuery = function () {
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
            opcode: "stats",
            table: table,
            filters: filters,
            field: this.statsFieldName
        });
    }
};

Stats.prototype.isSetupDone = function () {
    return this.statsFieldName && this.periodValue != 0 && this.periodUnit;
};

Stats.prototype.configChanged = function () {
    var modal = $(this.setupModalName);
    this.table = modal.find(".tile-table").first().val();
    if (!this.table) {
        this.table = this.tables.selectedTable.name;
    }
    this.title = modal.find(".tile-title").val();
    this.periodUnit = modal.find(".tile-time-unit").first().val();
    this.periodValue = parseInt(modal.find(".tile-time-value").first().val());
    this.statsFieldName = modal.find(".stats-field").val();
    this.ignoreDigits = parseInt(modal.find(".ignored-digits").val());
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
    this.selectedStat = modal.find(".statistic_to_plot").val();
};

Stats.prototype.loadFieldList = function () {
    var modal = $(this.setupModalName);
    var selected_table_name = modal.find(".tile-table").first().val();
    console.log("Loading Field List for " + selected_table_name);
    var selected_table = extractSelectedTable(selected_table_name, this.tables.tables);
    var field_select = modal.find("#stats-field");
    field_select.find('option').remove();

    this.tables.loadTableMeta(selected_table, function () {
        for (var i = selected_table.mappings.length - 1; i >= 0; i--) {
            field_select.append('<option>' + selected_table.mappings[i].field + '</option>');
        }

        if (this.statsFieldName) {
            field_select.val(this.statsFieldName);
        }
        field_select.selectpicker('refresh');
    }.bind(this));
};


Stats.prototype.populateSetupDialog = function () {
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
    modal.find('.statistic_to_plot').first().val(this.selectedStat);
    modal.find('.statistic_to_plot').first().selectpicker("refresh");
    modal.find(".ignored-digits").val(this.ignoreDigits);
};

Stats.prototype.registerSpecificData = function (representation) {
    representation['periodUnit'] = this.periodUnit;
    representation['periodValue'] = this.periodValue;
    representation['statsFieldName'] = this.statsFieldName;
    if (this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
    representation['selectedStat'] = this.selectedStat;
};

Stats.prototype.loadSpecificData = function (representation) {
    this.periodUnit = representation['periodUnit'];
    if (!this.periodUnit) {
        this.periodUnit = "minutes";
    }
    if (representation['period']) {
        this.periodValue = representation['period'];
    } else {
        this.periodValue = representation['periodValue'];
    }

    this.statsFieldName = representation['statsFieldName'];
    if (representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
    this.selectedStat = representation['selectedStat'];
};