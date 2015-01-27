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

function StatsTrend () {
	this.typeName = "statstrend";
	this.refresh = true;
	this.setupModalName = "#setupStatsTrendChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
	this.period = 0;
    this.selectedFilters = null;
    this.selectedStats = [];

}

function deepFind(obj, path) {
  var paths = path.split('.')
    , current = obj
    , i;

  for (i = 0; i < paths.length; ++i) {
    if (current[paths[i]] == undefined) {
      return undefined;
    } else {
      current = current[paths[i]];
    }
  }
  return current;
}

StatsTrend.prototype = new Tile();

StatsTrend.prototype.render = function(data, animate) {
    if (this.title){
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Stats for " + this.eventTypeFieldName);
    }

    var parent = $("#content-for-" + this.id);
    var canvas = null;
    if(!parent || 0 == parent.find(".chartcanvas").length) {
        //$("#content-for-" + this.id).append("<div class='chart-content'/>");
        parent = $("#content-for-" + this.id);//.find(".chart-content");
        //parent.append("<div style='height: 15%'><input type='text' class='form-control col-lg-12 eventfilter' placeholder='Start typing here to filter event type...'/></div>");
        canvas = $("<div>", {class: "chartcanvas"});
        parent.append(canvas);
        legendArea = $("<div>", {class: "legendArea"});
        //legendArea.height("10%");
        //legendArea.width("100%");
        parent.append(legendArea)
    }
    else {
        canvas = parent.find(".chartcanvas");
    }
	/*if(!parent || 0 == parent.find(".chartcanvas").length) {
        $("#content-for-" + this.id).append("<div class='chart-content'/>");
        parent = $("#content-for-" + this.id);//.find(".chart-content");
    	//parent.append("<div style='height: 15%'><input type='text' class='form-control col-lg-12 eventfilter' placeholder='Start typing here to filter event type...'/></div>");
        canvas = $("<div>", {class: "chartcanvas"});
		parent.append(canvas);
	}
	else {
		canvas = parent.find(".chartcanvas");
	}*/
	if(!data.hasOwnProperty("result")) {
		canvas.empty();
		return;
	}

	var results = data.result;
    var selectedStats = this.selectedStats;
    var colors = new Colors(selectedStats.length);
    var d = [];
    var colorIdx = 0;
    for(var j = 0; j < selectedStats.length; j++) {
        d.push({ data: [], color: colors[colorIdx], label : selectedStats[j], lines: {show: true}, shadowSize: 0/*, curvedLines: {apply: true}*/});
    }
    var colorIdx = 0;
    var timestamp = new Date().getTime();
    var tmpData = new Object();
    for(var i = 0; i < results.length; i++) {
        var stats = results[i].stats;
        var percentiles = results[i].percentiles;
        for(var j = 0; j < selectedStats.length; j++) {
            var selected = selectedStats[j];
            var value = 0;
            if(selected.startsWith('percentiles.')) {
                value = percentiles[selected.split("percentiles.")[1]];
            }
            if(selected.startsWith('stats.')) {
                value = stats[selected.split("stats.")[1]];
            }
            d[j].data.push([results[i].period, value]);
        }
    }

    /*for(var trend in data.trends) {
        var trendData = data.trends[trend];
        for (var i = 0; i < trendData.length; i++) {
            var time = trendData[i].period;
            var count = trendData[i].count;
            if(!tmpData.hasOwnProperty(time)) {
                tmpData[time] = new Object();
            }
            tmpData[time][trend] = count;
        }
    }
    if(0 == Object.keys(tmpData).length) {
        canvas.empty();
        return;
    }

    var trendWiseData = new Object();
    for(var time in tmpData) {
        for(var trend in data.trends) {
            if(regexp && !regexp.test(trend)) {
                continue;
            }
            var count = 0;
            var timeData = tmpData[time];
            if(timeData.hasOwnProperty(trend)) {
                count = timeData[trend];
                console.log("time found ");
            }
            var rows = null;
            if(!trendWiseData.hasOwnProperty(trend)) {
                rows = [];
                trendWiseData[trend] = rows;
            }
            rows = trendWiseData[trend];
            var timeVal = parseInt(time);
            rows.push([timeVal, count]);
        }
    }
    console.log(trendWiseData);
    for(var trend in trendWiseData) {
        var rows = trendWiseData[trend];
        if(regexp && !regexp.test(trend)) {
            continue;
        }
        rows.sort(function(lhs, rhs) {
            return (lhs[0] < rhs[0]) ? -1 : ((lhs[0] == rhs[0])? 0 : 1);
        })
        d.push({ data: rows, color: colors[colorIdx], label : trend, fill: true, fillColor: colors[colorIdx] });
    }*/
    $.plot(canvas, d, {
            series: {
                //stack: true,
                lines: {
                    show: true,
                    fill: 0,
                    lineWidth: 2.0,
                    fillColor: { colors: [{ opacity: 0.7 }, { opacity: 0.1}]}
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
                timezone: "browser"
            },
            selection : {
                mode: "x",
                minSize: 1
            },
            tooltip: true,
            tooltipOpts: {
                content: function(label, x, y) {
                           var date = new Date(x);
                           return label + ": " + y.toFixed(2) + "ms at " + date.getHours() + ":" + date.getMinutes();
                         },
                defaultFormat: true
            },
            legend: {
                show:true,
                position: 'e',
                noColumns: 8,
                noRows: 0,
                labelFormatter: function(label, series){
                    return '<font color="black"> &nbsp;' + label +' &nbsp;</font>';
                },
                container: parent.find(".legendArea")
            }
        });
};

StatsTrend.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
        var filters = [];
        filters.push(timeValue(this.period, $("#" + this.id).find(".period-select").val()));
        if(this.selectedFilters && this.selectedFilters.filters){
            for(var i = 0; i<this.selectedFilters.filters.length; i++){
                filters.push(this.selectedFilters.filters[i]);
            }
        }
		return JSON.stringify({
			opcode : "statstrend",
			table : this.tables.selectedTable.name,
			filters : filters,
			field : this.eventTypeFieldName,
            period: periodFromWindow($("#" + this.id).find(".period-select").val())
		});
	}
};

StatsTrend.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

StatsTrend.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
    this.title = modal.find(".tile-title").val()
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".statstrend-bar-chart-field").val();
    var filters = modal.find(".selected-filters").val();
    if(filters != undefined && filters != ""){
        var selectedFilters = JSON.parse(filters);
        if(selectedFilters != undefined){
            this.selectedFilters = selectedFilters;
        }
    }
    else {
        this.selectedFilters = null;
    }
    this.selectedStats = modal.find(".stats_to_plot").val();
};

StatsTrend.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	var select = $("#statstrend-bar-chart-field");
    this.title = modal.find(".tile-title").val()
	select.find('option').remove();
	for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
		select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
	};
	if(this.eventTypeFieldName) {
		select.val(this.eventTypeFieldName);
	}
	select.selectpicker('refresh');
	modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");
    if(this.selectedFilters){
       modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find('stats_to_plot').multiselect('select', this.selectedStats);
}

StatsTrend.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
    if(this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }
    representation['selectedStats'] = this.selectedStats;
};

StatsTrend.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
    if(representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
    this.selectedStats = representation['selectedStats'];
};