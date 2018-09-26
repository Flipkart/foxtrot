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

function axisTimeFormatNew(periodUnit, customPeriod) { // different time format
  var period = periodFromWindow(periodUnit, customPeriod);
  if (period == "hours" || period == "minutes") {
    return "HH:mm DD MMM";
  }
  if (period == "days") {
    return "DD MMM";
  }
  return "HH:MM ss";
}

function findIndex(currentTabName) { // index of given tab name
  var index = -1;
  for (var i = 0; i < globalData.length; i++) {
    for (var indexData in globalData[i]) {
      if (indexData == currentTabName) index = i;
      break;
    }
  }
  return index;
}

function numDifferentiation(val) { // indian numbers conversion
  if(val >= 10000000) val = parseFloat((val/10000000).toFixed(1)) + 'Cr';
  else if(val >= 100000) val = parseFloat((val/100000).toFixed(1)) + 'L';
  else if(val >= 1000) val = parseFloat((val/1000).toFixed(1)) + 'K';
  else
    val = parseFloat(val).toFixed(0);
  return val;
}

function generateDropDown(fields, element) { // generating all dropdowns
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

function getWidgetType() { // widget types
  if (currentChartType == "line" || currentChartType == "stacked" || currentChartType == "stackedBar" || currentChartType == "statsTrend" || currentChartType == "bar" || currentChartType == "lineRatio") {
    return "full";
  }
  else if (currentChartType == "radar" || currentChartType == "pie") {
    return "medium";
  }
  else if (currentChartType == "gauge" || currentChartType == "percentageGauge"  || currentChartType == "trend" || currentChartType == "count") {
    return "small";
  }
  else {
    return false;
  }
}

function getFilters() { // returns filter values
  var filterDetails = [];
  for (var filter = 0; filter < filterRowArray.length; filter++) {
    var filterId = filterRowArray[filter];
    var el = $("#filter-row-" + filterId);
    var filterColumn = $(el).find("select.filter-column").val();
    var filterType = $(el).find("select.filter-type-"+filter).val();
    var filterValue = $(el).find(".filter-value").val();
    var filterObject;
    if(filterType == "in" || filterType == "not_in") {
      filterValue = filterValue.split(',');
      filterObject = {
        "operator": filterType
        , "values": filterValue
        , "field": currentFieldList[parseInt(filterColumn)].field
      }
    }else if(filterType == "exists") {
      filterObject = {
        "operator": filterType
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
function getChartFormValues() { // get current widget form values
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
  else if (currentChartType == "percentageGauge") {
    return getPercentageGaugeChartFormValues();
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
  else if(currentChartType == "lineRatio") {
    return getLineRatioChartFormValues();
  }
}
function deleteFilterRow(el) { // delete given filter row
  var parentRow = $(el).parent();
  var parentRowId = parentRow.attr('id');
  var getRowId = parentRowId.split('-');
  var rowId = getRowId[2];
  filterRowArray = jQuery.grep(filterRowArray, function (value) {
    return value != rowId;
  });
  $(parentRow).remove();
}
function setFilters(object) { // setter for filters
  for (var filter = 0; filter <object.length; filter++) {
    var filterId = filter;
    var el = $("#filter-row-" + filterId);
    var fieldDropdown = $(el).find(".filter-column").val(currentFieldList.findIndex(x => x.field == object[filter].field));
    $(fieldDropdown).selectpicker('refresh');
    $(fieldDropdown).trigger('change');
    var operatorDropdown = $(el).find(".filter-type-"+filter).val(object[filter].operator);
    $(operatorDropdown).selectpicker('refresh');
    var filterValue;

    if(object[filter].value == undefined) {
      filterValue = object[filter].values;
    } else {
      filterValue = object[filter].value;
    }

    // hide value element if operator is exists
    if(object[filter].operator == "exists") {
      $(el).find(".filter-value").hide();
    } else {
      $(el).find(".filter-value").show();
    }

    // Ignore for exist
    if(filterValue) {
      if(filterValue.isArray ) {
        $(el).find(".filter-value").val(filterValue.toString());
      } else {
        $(el).find(".filter-value").val(filterValue);
      }
    }
    $(operatorDropdown).selectpicker('refresh');
  }
}
function reloadDropdowns() { // change dropdown values for all charts when table changes
  if (currentChartType == "line") {
    generateDropDown(currentFieldList, "#uniqueKey");
  }
  else if (currentChartType == "trend") {
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
  else if (currentChartType == "percentageGauge") {
    generateDropDown(currentFieldList, "#percentage-gauge-nesting");
    generateDropDown(currentFieldList, "#percentage-gauge-uniquekey");
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
  else if (currentChartType == "lineRatio") {
    generateDropDown(currentFieldList, "#line-ratio-field");
    generateDropDown(currentFieldList, "#line-ratio-uniquekey");
  }
}

function invokeClearChartForm() { // clear widget forms
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
  else if (currentChartType == "percentageGauge") {
    clearPercentageGaugeChartForm();
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
  else if(currentChartType == "lineRatio") {
    clearLineRatioChartForm();
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

function numberWithCommas(x) { // Indian numbers without thousands/lakhs in the end
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

function deleteWidget(id) { // delete widget
  showHideSideBar();

  // delete
  var deleteRow = tileData[id].tileContext.row;
  var deletePosition = tileData[id].tileContext.position;
  delete tileData[id];

  var idx = tileList.indexOf(id);
  if (idx != -1) tileList.splice(idx, 1);

  var count = 0; // count is for more than two div in a row
  for (var i in tileList) {
    if(tileList[i] != id) {
      var tileRow = tileData[tileList[i]].tileContext.row;
      var rowPosition = tileData[tileList[i]].tileContext.position;
      if(tileRow == deleteRow) {


        if(rowPosition > deletePosition) { // change position if row pos is greater than delete
          tileData[tileList[i]].tileContext.position = tileData[tileList[i]].tileContext.position - 1;
        }

        if(rowPosition > deletePosition && count == 0) { // if first column deleted set first item to new row
          tileData[tileList[i]].tileContext.isnewRow = true;
        }

        count++;
        tileData[tileList[i]].tileContext.row = tileData[tileList[i]].tileContext.row;
        }
      else if(tileRow > deleteRow && count == 0) {
          console.log(tileData[tileList[i]].id +" => "+tileData[tileList[i]].tileContext.row)
          tileData[tileList[i]].tileContext.row = tileData[tileList[i]].tileContext.row -1;
        }
      }
    }

  /* sort array list */
  var keysSorted = sortTiles(tileData);
  tileList = [];
  tileList = keysSorted;
  globalData[getActiveTabIndex()].tileList = keysSorted;
  renderAfterRearrange();
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

function drawLegend(columns, element) { // legend
  if(!element) {
    return;
  }
  columns.sort(function (lhs, rhs){
    return rhs.data - lhs.data;
  });
  element.html(handlebars("#group-legend-template", {data: columns}));
}

function drawPieLegend(columns, element) { // pie legend
  if(!element) {
    return;
  }
  columns.sort(function (lhs, rhs){
    return rhs.data - lhs.data;
  });
  element.html(handlebars("#group-legend-pie-template", {data: columns}));
}

function convertName(name) { // convert given name into machine readable
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

function convertHex(hex,opacity){ // converting given hexa decial value to rgb
  hex = hex.replace('#','');
  r = parseInt(hex.substring(0,2), 16);
  g = parseInt(hex.substring(2,4), 16);
  b = parseInt(hex.substring(4,6), 16);

  result = 'rgba('+r+','+g+','+b+','+opacity/100+')';
  return result;
}

function getWidgetSize(type) { // widget types
  if (type == "line" || type == "stacked" || type == "stackedBar" || type == "statsTrend" || type == "bar" || type == "lineRatio") {
    return 12;
  }
  else if (type == "radar" || type == "pie") {
    return 6;
  }
  else if (type == "gauge" || type == "percentageGauge"  || type == "trend" || type == "count") {
    return 3;
  }
  else {
    return 0;
  }
}