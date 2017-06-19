// Return data format like yyyy-mm-dd
function formatDate(date) {
  var d = new Date(date)
    , month = '' + (d.getMonth() + 1)
    , day = '' + d.getDate()
    , year = d.getFullYear();
  if (month.length < 2) month = '0' + month;
  if (day.length < 2) day = '0' + day;
  //[year, month, day].join('-');
  return day;
}

function periodFromWindow(periodUnit, customPeriodString) {
  if (!customPeriodString) {
    return "days";
  }

  if (!customPeriodString) {
    return "days";
  }

  if (customPeriodString == "custom") {
    return periodUnit;
  }

  if (customPeriodString.endsWith("m")) {
    return 'minutes';
  }
  if (customPeriodString.endsWith("h")) {
    return "hours";
  }
  if (customPeriodString === "1d") {
    return "hours";
  }
  if (customPeriodString.endsWith("d")) {
    return "days";
  }
  return "days";
}

function timeValue(periodUnit, periodValue, selectedPeriodString) {
  var timestamp = new Date().getTime();
  if (selectedPeriodString === "custom" || !selectedPeriodString) {
    return {
      field: "_timestamp"
      , operator: "last"
      , duration: periodValue + periodUnit
      , currentTime: timestamp
    };
  }
  return {
    operator: "last"
    , duration: selectedPeriodString
  };
}

function getPeriodString(periodUnit, periodValue, selectedPeriodString) {
  if (selectedPeriodString === "custom") {
    return periodValue + labelPeriodString(periodUnit);
  }
  return selectedPeriodString;
}

function labelPeriodString(periodString) {
  if (!periodString) {
    return null;
  }
  if (periodString == "days") {
    return "d";
  }
  else if (periodString == "hours") {
    return "h";
  }
  else {
    return "m";
  }
}

function axisTimeFormat(periodUnit, customPeriod) {
  var period = periodFromWindow(periodUnit, customPeriod);
  if (period == "hours" || period == "minutes") {
    return "%H:%M %e %b";
  }
  if (period == "days") {
    return "%e %b";
  }
  return "%e %b";
}

function axisTimeFormatNew(periodUnit, customPeriod) {
  var period = periodFromWindow(periodUnit, customPeriod);
  if (period == "hours" || period == "minutes") {
    return "HH:mm ss";
  }
  if (period == "days") {
    return "DD MMM";
  }
  return "HH:MM ss";
}

function findIndex(currentTabName) {
  var index = -1;
  for (var i = 0; i < globalData.length; i++) {
    for (var indexData in globalData[i]) {
      if (indexData == currentTabName) index = i;
      break;
    }
  }
  return index;
}

function numDifferentiation(val) {
  if(val >= 10000000) val = parseFloat((val/10000000).toFixed(1)) + 'Cr';
  else if(val >= 100000) val = parseFloat((val/100000).toFixed(1)) + 'L';
  else if(val >= 1000) val = parseFloat((val/1000).toFixed(1)) + 'K';
  return val;
}

function generateDropDown(fields, element) {
  var el = $(element);
  var arr = fields;
  el.find('option').remove();

  $(el).append($('<option>', {
    value: ""
    , text: "none"
  }));

  $.each(arr, function(key, value) {
    $(el).append($("<option></option>")
                 .attr("value",key)
                 .text(value.field));
  });
  $(el).selectpicker('refresh');
}

function getWidgetType() {
  if (currentChartType == "line" || currentChartType == "stacked" || currentChartType == "stackedBar" || currentChartType == "statsTrend" || currentChartType == "bar") {
    return "full";
  }
  else if (currentChartType == "radar" || currentChartType == "pie") {
    return "medium";
  }
  else if (currentChartType == "gauge" || currentChartType == "trend" || currentChartType == "count") {
    return "small";
  }
  else {
    return false;
  }
}

function getFilters() {
  var filterDetails = [];
  for (var filter = 0; filter < filterRowArray.length; filter++) {
    var filterId = filterRowArray[filter];
    var el = $("#filter-row-" + filterId);
    var filterColumn = $(el).find("select.filter-column").val();
    var filterType = $(el).find("select.filter-type").val();
    var filterValue = $(el).find(".filter-value").val();
    var filterObject;
    if(filterType == "in" || filterType == "not_in") {
      filterValue = filterValue.split(',');
      filterObject = {
        "operator": filterType
        , "values": filterValue
        , "field": currentFieldList[parseInt(filterColumn)].field
      }
    } else {
      filterObject = {
        "operator": filterType
        , "value": filterValue
        , "field": currentFieldList[parseInt(filterColumn)].field
      }
    }
    filterDetails.push(filterObject);
  }
  return filterDetails;
}
function getChartFormValues() {
  if (currentChartType == "line") {
    return getLineChartFormValues();
  }
  else if (currentChartType == "trend") {
    return getTrendChartFormValues();
  }
  else if (currentChartType == "stacked") {
    return getstackedChartFormValues();
  }
  else if (currentChartType == "radar") {
    return getRadarChartFormValues();
  }
  else if (currentChartType == "gauge") {
    return getGaugeChartFormValues();
  }
  else if (currentChartType == "stackedBar") {
    return getstackedBarChartFormValues();
  }
  else if (currentChartType == "pie") {
    return getPieChartFormValues();
  }
  else if(currentChartType == "statsTrend") {
    return getStatsTrendTileChartFormValues();
  }
  else if(currentChartType == "bar") {
    return getBarChartFormValues();
  }
  else if(currentChartType == "count") {
    return getCountChartFormValues();
  }
}
function deleteFilterRow(el) {
  var parentRow = $(el).parent();
  var parentRowId = parentRow.attr('id');
  var getRowId = parentRowId.split('-');
  var rowId = getRowId[2];
  filterRowArray = jQuery.grep(filterRowArray, function (value) {
    return value != rowId;
  });
  $(parentRow).remove();
}
function setFilters(object) {
  setTimeout(function(){
    for (var filter = 0; filter <object.length; filter++) {
      var filterId = filter;
      var el = $("#filter-row-" + filterId);
      var fieldDropdown = $(el).find(".filter-column").val(currentFieldList.findIndex(x => x.field == object[filter].field));
      var operatorDropdown = $(el).find(".filter-type").val(object[filter].operator);
      var filterValue;

      if(object[filter].value == undefined) {
        filterValue = object[filter].values;
      } else {
        filterValue = object[filter].value;
      }

      if(filterValue.isArray ) {
        $(el).find(".filter-value").val(filterValue.toString());
      } else {
        $(el).find(".filter-value").val(filterValue);
      }

      $(fieldDropdown).selectpicker('refresh');
      $(operatorDropdown).selectpicker('refresh');
    }
  }, 1000);

}
function reloadDropdowns() {
  if (currentChartType == "line") {
    generateDropDown(currentFieldList, "#uniqueKey");
  }
  else if (currentChartType == "trend") {
    console.log(currentFieldList);
    generateDropDown(currentFieldList, "#stats-field");
  }
  else if (currentChartType == "stacked") {
    generateDropDown(currentFieldList, "#stacking-key");
    generateDropDown(currentFieldList, "#stacked-uniquekey");
    generateDropDown(currentFieldList, "#stacked-grouping-key");
  }
  else if (currentChartType == "radar") {
    generateDropDown(currentFieldList, "#radar-nesting");
  }
  else if (currentChartType == "gauge") {
    generateDropDown(currentFieldList, "#gauge-nesting");
  }
  else if (currentChartType == "stackedBar") {
    generateDropDown(currentFieldList, "#stacked-bar-field");
    generateDropDown(currentFieldList, "#stacked-bar-uniquekey");
  }
  else if (currentChartType == "pie") {
    generateDropDown(currentFieldList, "#eventtype-field");
    generateDropDown(currentFieldList, "#pie-uniquekey");
  }
  else if(currentChartType == "statsTrend") {
    generateDropDown(currentFieldList, "#stats-trend-field");
  }
  else if(currentChartType == "bar") {
    generateDropDown(currentFieldList, "#bar-event-field");
    generateDropDown(currentFieldList, "#bar-uniquekey");
  }
  else if(currentChartType == "count") {
    generateDropDown(currentFieldList, "#count-field");
  }
}

function invokeClearChartForm() {
  if (currentChartType == "line") {
    clearLineChartForm();
  }
  else if (currentChartType == "trend") {
    clearTrendChartForm();
  }
  else if (currentChartType == "stacked") {
    clearstackedChartForm();
  }
  else if (currentChartType == "radar") {
    clearRadarChartForm();
  }
  else if (currentChartType == "gauge") {
    clearGaugeChartForm();
  }
  else if (currentChartType == "stackedBar") {
    clearStackedBarChartForm();
  }
  else if (currentChartType == "pie") {
    clearPieChartForm();
  }
  else if(currentChartType == "statsTrend") {
    clearStatsTrendTileChartForm();
  }
  else if(currentChartType == "bar") {
    clearBarChartForm();
  }
  else if(currentChartType == "count") {
    clearCountChartForm();
  }
}

// Get the remaining array between two arrays having same values
function arr_diff (a1, a2) {
  var a = [], diff = [];
  for (var i = 0; i < a1.length; i++) {
    a[a1[i]] = true;
  }
  for (var i = 0; i < a2.length; i++) {
    if (a[a2[i]]) {
      delete a[a2[i]];
    } else {
      a[a2[i]] = true;
    }
  }
  for (var k in a) {
    diff.push(k);
  }
  return diff;
};

function unique(list) {
  var result = [];
  $.each(list, function (i, e) {
    if ($.inArray(e, result) == -1) result.push(e);
  });
  return result;
}

function numberWithCommas(x) {
  x=x.toString();
  var afterPoint = '';
  if(x.indexOf('.') > 0)
    afterPoint = x.substring(x.indexOf('.'),x.length);
  x = Math.floor(x);
  x=x.toString();
  var lastThree = x.substring(x.length-3);
  var otherNumbers = x.substring(0,x.length-3);
  if(otherNumbers != '')
    lastThree = ',' + lastThree;
  var res = otherNumbers.replace(/\B(?=(\d{2})+(?!\d))/g, ",") + lastThree + afterPoint;
  return res;
}

function deleteWidget(id) {
  showHideSideBar();
  delete tileData[id];
  var idx = tileList.indexOf(id);
  if (idx != -1) tileList.splice(idx, 1);
}

function getPeroidSelectString(string) {
  if(string == "minutes") {
    return 'm';
  } else if(string == "hours") {
    return 'h';
  } else if(string == "days") {
    return 'd';
  }
}

function drawLegend(columns, element) {
  if(!element) {
    return;
  }
  columns.sort(function (lhs, rhs){
    return rhs.data - lhs.data;
  });
  element.html(handlebars("#group-legend-template", {data: columns}));
}

function drawPieLegend(columns, element) {
  if(!element) {
    return;
  }
  columns.sort(function (lhs, rhs){
    return rhs.data - lhs.data;
  });
  element.html(handlebars("#group-legend-pie-template", {data: columns}));
}

function convertName(name) {
  return name.trim().toLowerCase().split(' ').join("_");
}

function getFullWidgetClassName(size) {
  if(size == 9) {
    return 'col-sm-9';
  } else if(size == 6) {
    return 'col-sm-8'
  } else {
    return 'col-sm-10';
  }
}

function fullWidgetChartHeight() {
  return 290;
}

function convertHex(hex,opacity){
  hex = hex.replace('#','');
  r = parseInt(hex.substring(0,2), 16);
  g = parseInt(hex.substring(2,4), 16);
  b = parseInt(hex.substring(4,6), 16);

  result = 'rgba('+r+','+g+','+b+','+opacity/100+')';
  return result;
}
