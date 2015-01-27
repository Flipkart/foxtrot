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

function DonutTile () {
     this.typeName = "donut";
     this.refresh = true;
     this.setupModalName = "#setupPieChartModal";
     //Instance properties
     this.eventTypeFieldName = null;
     this.selectedValues = null;
     this.period = 0;
    this.selectedFilters = null;
    this.uniqueValues = [];
    this.uiFilteredValues;
    this.showLegend = false;
}

DonutTile.prototype = new Tile();

DonutTile.prototype.render = function(data, animate) {
    if (this.title){
        $("#" + this.id).find(".tile-header").text(this.title);
    } else {
        $("#" + this.id).find(".tile-header").text("Group by " + this.eventTypeFieldName);
    }
     var parent = $("#content-for-" + this.id);

     var chartLabel = null;
     if(0 == parent.find(".pielabel").length) {
          chartLabel = $("<div>", {class: "pielabel"});
          parent.append(chartLabel);
     }
     else {
          chartLabel = parent.find(".pielabel");
     }
    chartLabel.text(getPeriodString(this.period, $("#" + this.id).find(".period-select").val()));

     var canvas = null;
     var legendArea = null;
     if(this.showLegend) {
         if(0 == parent.find(".chartcanvas").length) {
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
       if(0 == parent.find(".chartcanvas").length) {
     		canvas = $("<div>", {class: "chartcanvas"});
     		parent.append(canvas);
     	}
     	else {
     		canvas = parent.find(".chartcanvas");
     	}
     }

    if(!data.hasOwnProperty("result")) {
          canvas.empty();
          return;
     }

     var colors = new Colors(Object.keys(data.result).length);
     var columns =[];
	this.uniqueValues = [];
     for(property in data.result) {
	    if(this.isValueVisible(property)) {
    		columns.push({label: property, data: data.result[property], color: colors.nextColor(), lines: {show:true}, shadowSize: 0});
	    }
	    this.uniqueValues.push(property);
	}
	var chartOptions = {
        series: {
            pie: {
                innerRadius: 0.8,
                show: true,
                label:{
                    show: false
                }
            }
        },
        legend : {
            show: false
        },
        grid: {
             hoverable: true
        },
        tooltip: true,
        tooltipOpts: {
              content: function(label, x, y) {
                   return label + ": " + y;
              }
         }
    };
    $.plot(canvas, columns, chartOptions);
    drawLegend(columns, legendArea);
     // var chart = c3.generate({
     //      bindto: chartAreaId,
     //      size: {
     //           height: parentHeight,
     //           width: parentWidth
     //      },
     //      data: {
     //           columns: columns,
     //           type : 'pie'
     //      },
     //      legend: {
     //           show: false
     //      },
     //      transition: {
     //           duration: transitionTime
     //      }

     // });

};
 
DonutTile.prototype.getQuery = function() {
     if(this.eventTypeFieldName && this.period != 0) {
          var timestamp = new Date().getTime();
          var filters = [];
          filters.push(timeValue(this.period, $("#" + this.id).find(".period-select").val()));
        if(this.selectedValues) {
            filters.push({
                field: this.eventTypeFieldName,
                operator: "in",
                values: this.selectedValues
            });
        }
        if(this.selectedFilters && this.selectedFilters.filters){
            for(var i = 0; i<this.selectedFilters.filters.length; i++){
                filters.push(this.selectedFilters.filters[i]);
            }
        }
          return JSON.stringify({
               opcode : "group",
               table : this.tables.selectedTable.name,
               filters : filters,
               nesting : [this.eventTypeFieldName]
          });
     }
};

DonutTile.prototype.isSetupDone = function() {
     return this.eventTypeFieldName && this.period != 0;     
};

DonutTile.prototype.configChanged = function() {
     var modal = $(this.setupModalName);
     this.title = modal.find(".tile-title").val()
     this.period = parseInt(modal.find(".refresh-period").val());
     this.eventTypeFieldName = modal.find(".pie-chart-field").val();
     var values = modal.find(".selected-values").val();
     if(values) {
         this.selectedValues = values.replace(/ /g, "").split(",");
     }
     else {
         this.selectedValues = null;
     }
    var filters = modal.find(".selected-filters").val();
    if(filters != undefined && filters != ""){
        var selectedFilters = JSON.parse(filters);
        if(selectedFilters != undefined){
            this.selectedFilters = selectedFilters;
        }
    }else{
        this.selectedFilters = null;
    }
    this.showLegend = modal.find(".pie-show-legend").prop('checked');
    $("#content-for-" + this.id).find(".chartcanvas").remove();
    $("#content-for-" + this.id).find(".pielabel").remove();
};

DonutTile.prototype.populateSetupDialog = function() {
     var modal = $(this.setupModalName);
     modal.find(".tile-title").val(this.title)
     var select = $("#pie_field");
     select.find('option').remove();
     for (var i = this.tables.currentTableFieldMappings.length - 1; i >= 0; i--) {
          select.append('<option>' + this.tables.currentTableFieldMappings[i].field + '</option>');
     };
     if(this.eventTypeFieldName) {
          select.val(this.eventTypeFieldName);
     }
     select.selectpicker('refresh');
     modal.find(".refresh-period").val(( 0 != this.period)?this.period:"");
     if(this.selectedValues) {
        modal.find(".selected-values").val(this.selectedValues.join(", "));
     }
    if(this.selectedFilters){
       modal.find(".selected-filters").val(JSON.stringify(this.selectedFilters));
    }
    modal.find(".pie-show-legend").prop('checked', this.showLegend);
}

DonutTile.prototype.registerSpecificData = function(representation) {
     representation['period'] = this.period;
     representation['eventTypeFieldName'] = this.eventTypeFieldName;
     representation['selectedValues'] = this.selectedValues;
     representation['showLegend'] = this.showLegend;
    if(this.selectedFilters) {
        representation['selectedFilters'] = btoa(JSON.stringify(this.selectedFilters));
    }

};

DonutTile.prototype.loadSpecificData = function(representation) {
     this.period = representation['period'];
     this.eventTypeFieldName = representation['eventTypeFieldName'];
     this.selectedValues = representation['selectedValues'];;
    if(representation.hasOwnProperty('selectedFilters')) {
        this.selectedFilters = JSON.parse(atob(representation['selectedFilters']));
    }
     if(representation.hasOwnProperty('showLegend')) {
        this.showLegend = representation['showLegend'];
     }
};

DonutTile.prototype.isValueVisible = function(value) {
   return !this.uiFilteredValues || this.uiFilteredValues.hasOwnProperty(value);
}

DonutTile.prototype.getUniqueValues = function() {
    var options = [];
    for(var i = 0; i < this.uniqueValues.length; i++) {
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

DonutTile.prototype.filterValues = function(values) {
    if(!values || values.length == 0) {
        values = this.uniqueValues;
    }
    this.uiFilteredValues = new Object();
    for(var i = 0; i < values.length; i++) {
        this.uiFilteredValues[values[i]] = 1;
    }
}