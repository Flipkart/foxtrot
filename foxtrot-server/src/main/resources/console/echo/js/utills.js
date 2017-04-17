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
  if (customPeriodString == "custom") {
    return periodUnit;
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
  var textToInsert = [];
  var i = 0;
  var length = arr.length;
  for (var a = 0; a < length; a += 1) {
    textToInsert[i++] = '<option value=' + a + '>';
    textToInsert[i++] = arr[a].field;
    textToInsert[i++] = '</option>';
  }
  $(el).append($('<option>', {
    value: "none"
    , text: "none"
  }));
  $(el).append(textToInsert.join(''));
  $(el).selectpicker('refresh');
}

function getLegendColumn(widgetType) {
  if(widgetType == "full") {
    return 7;
  } else if(widgetType == "medium") {
    return 4;
  }
}

function getWidgetType() {
  if (currentChartType == "line" || currentChartType == "stacked" || currentChartType == "stackedBar") {
    return "full";
  }
  else if (currentChartType == "radar" || currentChartType == "pie") {
    return "medium";
  }
  else if (currentChartType == "gauge" || currentChartType == "trend") {
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
    var filterColumn = $(el).find(".filter-column").val();
    var filterType = $(el).find(".filter-type").val();
    var filterValue = $(el).find(".filter-value").val();
    var filterObject;
    if(filterType == "in") {
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
  for (var filter = 0; filter < filterRowArray.length; filter++) {
    var filterId = filterRowArray[filter];
    var el = $("#filter-row-" + filterId);
    var fieldDropdown = $(el).find(".filter-column").val(currentFieldList.findIndex(x => x.field == object[filter].field));
    var operatorDropdown = $(el).find(".filter-type").val(object[filter].operator);
    $(el).find(".filter-value").val(object[filter].value);
    $(fieldDropdown).selectpicker('refresh');
    $(operatorDropdown).selectpicker('refresh');
  }
}
function reloadDropdowns() {
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
  else if (currentChartType == "stackedBar") {
    generateDropDown(currentFieldList, "#stacked-bar-field");
    generateDropDown(currentFieldList, "#stacked-bar-uniquekey");
  }
  else if (currentChartType == "pie") {
    generateDropDown(currentFieldList, "#eventtype-field");
    generateDropDown(currentFieldList, "#pie-uniquekey");
  }
}
