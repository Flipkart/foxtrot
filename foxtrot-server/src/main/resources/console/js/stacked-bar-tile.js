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

function StackedBar () {
	this.typeName = "stacked_bar";
	this.refresh = true;
	this.setupModalName = "#setupStackedBarChartModal";
	//Instance properties
	this.eventTypeFieldName = null;
	this.period = 0;		
}

StackedBar.prototype = new Tile();

StackedBar.prototype.render = function(data, animate) {
    var parent = $("#content-for-" + this.id);

	var canvas = null;
	if(!parent || 0 == parent.find(".chartcanvas").length) {
        //$("#content-for-" + this.id).append("<div class='chart-content'/>");
        parent = $("#content-for-" + this.id);//.find(".chart-content");
    	parent.append("<div style='height: 15%'><input type='text' class='form-control col-lg-12 eventfilter' placeholder='Start typing here to filter event type...'/></div>");
        canvas = $("<div>", {class: "chartcanvas"});
		parent.append(canvas);
	}
	else {
		canvas = parent.find(".chartcanvas");
	}
	if(!data.hasOwnProperty("trends")) {
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
    if(filterField) {
        regexp = new RegExp(filterField, 'i');
    }

    for(var trend in data.trends) {
        if(regexp && !regexp.test(trend)) {
            continue;
        }
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
    //console.log(tmpData);
/*    for(var trend in data.trends) {
        var tmpData = new Object();
        var rows = [];
        rows.push(['date', 'count']);
        var trendData = data.trends[trend];
        for (var i = 0; i < trendData.length; i++) {
            tmpData[trendData[i].period] = trendData[i].count;
        };
        console.log(tmpData);
        for (var t = timestamp - (this.period * 60000); t <= timestamp; t += 60000) {
            if(tmpData.hasOwnProperty(t)) {
                rows.push([t, tmpData[t]]);
                console.log("Found: " + t);
            }
            else {
                rows.push([t, 0]);
            }
        }
        d.push({ data: rows, color: colors[colorIdx], label : trend, fill: true, fillColor: colors[colorIdx] });
        colorIdx++;
    }*/
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
    }
    $.plot(canvas, d, {
            series: {
                stack: true,
                lines: {show: true, fill: 1}
            },
            grid: {
                hoverable: true,
                color: "white",
                show: true
            },
            xaxis: {
                mode: "time",
                timezone: "browser",
                min: timestamp - (this.period * 60000),
                max: timestamp
            },
            selection : {
                mode: "x",
                minSize: 1
            },
            tooltip: true,
            tooltipOpts: {
                content: "%y events at %x",
                defaultFormat: true
            },
            legend: { show:true, position: 'nw', noColumns: 0, noRows: 0, labelFormatter: function(label, series){
                return '<font color="black"> &nbsp;' + label +' &nbsp;</font>';
            }}
        });
};

StackedBar.prototype.getQuery = function() {
	if(this.eventTypeFieldName && this.period != 0) {
		var timestamp = new Date().getTime();
		return JSON.stringify({
			opcode : "trend",
			table : this.tables.selectedTable.name,
			filters : [{
				field: "_timestamp",
				operator: "between",
				temporal: true,
				from: (timestamp - (this.period * 60000)),
				to: timestamp
			}],
			field : this.eventTypeFieldName,
            period: "minutes"
		});
	}
};

StackedBar.prototype.isSetupDone = function() {
	return this.eventTypeFieldName && this.period != 0;	
};

StackedBar.prototype.configChanged = function() {
	var modal = $(this.setupModalName);
	this.period = parseInt(modal.find(".refresh-period").val());
	this.eventTypeFieldName = modal.find(".stacked-bar-chart-field").val();
};

StackedBar.prototype.populateSetupDialog = function() {
	var modal = $(this.setupModalName);
	var select = modal.find(".stacked-bar-chart-field");
	select.find('option').remove();
	for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
		select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
	};
	if(this.eventTypeFieldName) {
		select.val(this.eventTypeFieldName);
	}
	select.selectpicker('refresh');
	modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");	
}

StackedBar.prototype.registerSpecificData = function(representation) {
	representation['period'] = this.period;
	representation['eventTypeFieldName'] = this.eventTypeFieldName;
};

StackedBar.prototype.loadSpecificData = function(representation) {
	this.period = representation['period'];
	this.eventTypeFieldName = representation['eventTypeFieldName'];
};