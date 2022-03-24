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
  var timestamp;
  if(isGlobalDateFilter) {
      timestamp = globalDateFilterValue;
  } else {
      timestamp = new Date().getTime();
  }
  if (selectedPeriodString === "custom" || !selectedPeriodString || isGlobalDateFilter) {
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

function generateFiltersDropDown(fields, element) { // generating all dropdowns
  var el = $(element);
  var arr = fields;
  el.find('option').remove();

  $(el).append($('<option>', {
    value: ""
    , text: "none"
  }));

  var option = "";
  $.each(arr, function(key, value) {
    option+= "<option value="+key+">"+value.field+"</option>"
  });
  $(el).append(option);
  $(el).selectpicker('refresh');
}

function generateDropDown(fields, element) { // generating all dropdowns
  
  var arr = fields;
  var option = "";
  $.each(arr, function(key, value) {
    option+= "<option value="+key+">"+value.field+"</option>"
  });

  for(var i = 0; i < element.length; i++) {
    $(element[i]).find('option').remove();
    $(element[i]).append($('<option>', {
      value: ""
      , text: "none"
    }));   
    $(element[i]).append(option);
    $(element[i]).selectpicker('refresh');
  }
}

function generateSunBurstDropDown(fields) { // generating all dropdowns
  var arr = fields;

  var option = "";
  $.each(arr, function(key, value) {
    option+= "<option value="+key+">"+value.field+"</option>"
  });
  
  for(var i = 1; i < 6; i++) {
    $("#sunburst-nesting-field"+i).find('option').remove();
    $("#sunburst-nesting-field"+i).append($('<option>', {
      value: ""
      , text: "none"
    }));   
    $("#sunburst-nesting-field"+i).append(option);
    $("#sunburst-nesting-field"+i).selectpicker('refresh');
  }
  
  var unique = $(".sunburstForm").find("#sunburst-uniqueKey");
  $(unique).find('option').remove();
  $(unique).append($('<option>', {
    value: ""
    , text: "none"
  }));   
  $(unique).append(option);
  $(unique).selectpicker('refresh');


  $('#sunburst-aggregation-field').append($('<option>', {
    value: ""
    , text: "none"
  }));   
  $("#sunburst-aggregation-field").append(option);
  $("#sunburst-aggregation-field").selectpicker('refresh');
  
}

function getWidgetType() { // widget types
  if (currentChartType == "line" || currentChartType == "geo_aggregation" || currentChartType == "chloropeth" || currentChartType == "s2grid" || currentChartType == "stacked" || currentChartType == "stackedBar" || currentChartType == "statsTrend" || currentChartType == "bar" || currentChartType == "lineRatio" || currentChartType == "sunburst" || currentChartType == "nonStackedLine") {
    return "full";
  }
  else if (currentChartType == "radar" || currentChartType == "pie" || currentChartType == "funnel") {
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
    }else if(filterType == "exists" || filterType == "missing") {
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
  switch (currentChartType) 
  {
      case "line": return getLineChartFormValues();
      case "geo_aggregation": return getMapChartFormValues();
      case "chloropeth": return getChloropethFormValues();
      case "s2grid": return getS2gridFormValues();
      case "trend": return getTrendChartFormValues();
      case "stacked": return getstackedChartFormValues();
      case "radar": return getRadarChartFormValues();
      case "gauge": return getGaugeChartFormValues();
      case "percentageGauge": return getPercentageGaugeChartFormValues();
      case  "stackedBar": return getstackedBarChartFormValues();
      case "pie": return getPieChartFormValues();
      case "statsTrend": return getStatsTrendTileChartFormValues();
      case "bar": return getBarChartFormValues();
      case "count": return getCountChartFormValues();
      case "lineRatio": return getLineRatioChartFormValues();
      case "sunburst":  return getSunburstChartFormValues();
      case "nonStackedLine":  return getNonStackedLineFormValues();
      case "funnel":return getFunnelChartFormValues();
      default: return {};
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
    if(object[filter].operator == "exists" || object[filter].operator == "missing") {
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
  switch (currentChartType) {
    case "line":
      generateDropDown(currentFieldList, ["#uniqueKey"]);
      break;
    case "geo_aggregation":
      generateDropDown(currentFieldList, ["#geo_aggregation-uniquekey",'#geo_aggregation-aggregation-field', "#geo_aggregation-event-field"]);
      break;
    case "trend":
      generateDropDown(currentFieldList, ["#stats-field"]);
      break;
    case "stacked":
      generateDropDown(currentFieldList, ["#stacking-key", "#stacked-uniquekey", "#stacked-grouping-key" ,'#stacked-aggregation-field']);
      break;
    case "radar":
      generateDropDown(currentFieldList, ["#radar-nesting",'#radar-aggregation-field' ]);
      break;
    case "gauge":
      generateDropDown(currentFieldList, ["#gauge-nesting",'#gauge-aggregation-field']);
      break;
    case "percentageGauge":
      generateDropDown(currentFieldList, ["#percentage-gauge-nesting", "#percentage-gauge-uniquekey",'#percentage-gauge-aggregation-field']);
      break;
    case "stackedBar":
      generateDropDown(currentFieldList, ["#stacked-bar-field", "#stacked-bar-uniquekey" ,'#stacked-bar-aggregation-field']);
      break;
    case "pie":
      generateDropDown(currentFieldList, ["#eventtype-field", "#pie-uniquekey" ,'#pie-aggregation-field']);
      break;
    case "statsTrend":
      generateDropDown(currentFieldList, ["#stats-trend-field"]);
      break;
    case "bar":
      generateDropDown(currentFieldList, ["#bar-event-field", "#bar-uniquekey",'#bar-aggregation-field']);
      break;
      case "chloropeth":
      generateDropDown(currentFieldList, ["#chloropeth-event-field", "#chloropeth-uniquekey",'#chloropeth-aggregation-field']);
      break;
      case "s2grid":
      generateDropDown(currentFieldList, ["#s2grid-event-field", "#s2grid-uniquekey",'#s2grid-aggregation-field']);
      break;
    case "count":
      generateDropDown(currentFieldList, ["#count-field"]);
      break;
    case "lineRatio":
      generateDropDown(currentFieldList, ["#line-ratio-field", "#line-ratio-uniquekey"]);
      break;
    case "sunburst":
      generateSunBurstDropDown(currentFieldList);
      break;
    case "nonStackedLine":
      generateDropDown(currentFieldList, ["#non-stacked-line-field", "#non-stacked-line-uniquekey"]);
      break;
      case "funnel":
        generateDropDown(currentFieldList, ["#funnel-field", "#funnel-uniquekey" ,'#funnel-aggregation-field']);
        break;
  }
}

function invokeClearChartForm() { // clear widget forms
  switch (currentChartType) 
  {
      case "line": clearLineChartForm(); break;
      case "trend": clearTrendChartForm(); break;
      case "stacked": clearstackedChartForm(); break;
      case "radar": clearRadarChartForm(); break;
      case "gauge": clearGaugeChartForm(); break;
      case "geo_aggregation": clearGeoAggregationChartForm(); break;
      case "chloropeth": clearChloropethForm(); break;
      case "s2grid": clearChloropethForm(); break;
      case "percentageGauge": clearPercentageGaugeChartForm(); break;
      case  "stackedBar": clearStackedBarChartForm(); break;
      case "pie": clearPieChartForm(); break;
      case "statsTrend": clearStatsTrendTileChartForm(); break;
      case "bar": clearBarChartForm(); break;
      case "count": clearCountChartForm(); break;
      case "lineRatio": clearLineRatioChartForm(); break;
      case "sunburst":  clearSunburstChartForm(); break;
      case "nonStackedLine":  clearNonStackedLineChartForm(); break;
      case "funnel":  clearfunnelChartForm(); break;
      default: return "";
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
  
  var sum = columns.reduce((s, f) => {
    return s + f.data;               // return the sum of the accumulator and the current time, as the the new accumulator
  }, 0); // calculate total value

  var percentage = _.map(columns, function(value, key){
    return ((value.data)/sum * 100).toFixed(1);
  }); // calculate sum for each value

  var final = _.each(columns, function(element, index) {
    _.extend(element, { "percentage" : percentage[index]});
  }); // extend percentage to original array

  element.html(handlebars("#group-legend-pie-template", {data: final}));
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

  switch (type) {
    case "line":
    
    case "stacked":
    case "stackedBar":
    case "statsTrend":
    case "bar":
    case "sunburst":
    case "lineRatio":
    case "nonStackedLine":
      return 12;
    case "radar":
    case "pie":
    case "funnel":
      case "geo_aggregation":
      case "chloropeth":
        case "s2grid":
      return 6;
    case "gauge":
    case "percentageGauge":
    case "trend":
    case "count":
      return 3;
    default:
      return 0;
  }
}

function thresholdErrorMsg() {
  return "Denominator value is below the threshold value. Hence, graph plotting not possible";
}

/**
 * Set cookie name
 */
function getCookieConstant() {
  return "ECHO_G_TOKEN";
}

/**
 * Get login redirect url
 */
function getLoginRedirectUrl() {

  var hostname = window.location.hostname;
  var redirectUrl = encodeURIComponent(window.location.href);
  return 0;
}

/**
 * Read cookie to check user is logged in or not
 * @param {*} cname 
 */
function getCookie(cname) {
  var name = cname + "=";
  var decodedCookie = decodeURIComponent(document.cookie);
  var ca = decodedCookie.split(';');
  for(var i = 0; i <ca.length; i++) {
    var c = ca[i];
    while (c.charAt(0) == ' ') {
      c = c.substring(1);
    }
    if (c.indexOf(name) == 0) {
      return c.substring(name.length, c.length);
    }
  }
  return "";
}

/**
 * Check user is logged in
 */
function isLoggedIn() {
    return true; // logged in
}

/**
 * Sort non stacked and stacked line chart legends
 * @param {*} d 
 * @param {*} element 
 */
function drawStackedLinesLegend(d, element) { // pie legend
  if(!element) {
    return;
  }
  var sortingReference = [];
  for( var i = 0; i < d.length; i++) {
    var value = d[i].data[0][1];
    var name = d[i].label;
    sortingReference.push({ "value": value, "name": name, color: d[i].color});
  }
  sortingReference.sort( function( a, b ) { return b.value - a.value; } )
  element.html(handlebars("#stacked-lines-legend-template", {data: sortingReference}));
}

function getLogoutUrl() {
  var hostname = window.location.hostname;
   return "0";
}

/**
 * submit logout form
 */
$("#logout-icon").click(function(){
  var logoutUrl = getLogoutUrl();
  if(logoutUrl != 0) { // prevent for local
    $("#logout").submit();
  }
});

$('#logout').attr('action', getLogoutUrl());



function todayyesterdaydbyesterday(object){

  // -------------- Starts added today yesterday and daybefore yesterday---------------

  var filtertoday = "";
  var timestamp = new Date().getTime();

  var filterdate = new Date().getDate();
  var filtermonth =new Date().getMonth();
  var filteryear =new Date().getFullYear();
  var timezeroToday = new Date(filteryear ,filtermonth ,filterdate).getTime();
  var timezeroYesterday = new Date(filteryear ,filtermonth,  filterdate-1).getTime();
  var timezeroBDYesterday = new Date(filteryear ,filtermonth,  filterdate-2).getTime();

  var timeendYesterday = timezeroYesterday +86300000;         //86300000 is one day timestamp value 
  var timeendBDYesterday = timezeroBDYesterday +86300000;    


  if(globalFilters) {
    filtertoday=getGlobalFilters();
    // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    filtertoday=getPeriodSelect(object.id);
    // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

if (filtertoday=== "1t") {
  filters.push({
    field: "time",
    operator: "between",
    from:timezeroToday,
    to:timestamp,
  });
}
else if(filtertoday=== "2y"){
  filters.push({
    field: "time",
    operator: "between",
    from:timezeroYesterday,
    to:timeendYesterday,
  });
}
else if(filtertoday=== "3dby"){
  filters.push({
    field: "time",
    operator: "between",
    from:timezeroBDYesterday,
    to:timeendBDYesterday,
  });
}
else{
  // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, filtertoday))

  if(globalFilters) {
    // filtertoday=getGlobalFilters();
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    // filtertoday=getPeriodSelect(object.id);
    filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

}

 }

//  -------- Starts Added today tomorrow day before yesterday for download and chart rendering -------------

const todayTomorrow =  function(filters_arr, gf_obj,get_gf,get_ps,tv_fn,filter_obj ) {
  var filtertoday = "";
  var timestamp = new Date().getTime();

  var filterdate = new Date().getDate();
  var filtermonth =new Date().getMonth();
  var filteryear =new Date().getFullYear();
  var timezeroToday = new Date(filteryear ,filtermonth ,filterdate).getTime();
  var timezeroYesterday = new Date(filteryear ,filtermonth,  filterdate-1).getTime();
  var timezeroBDYesterday = new Date(filteryear ,filtermonth,  filterdate-2).getTime();

  var timeendYesterday = timezeroYesterday +86400000;         //86400000 is one day timestamp value 24 hours
  var timeendBDYesterday = timezeroBDYesterday +86400000;    


  if(gf_obj) {
    filtertoday=get_gf();
    // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getGlobalFilters()))
  } else {
    filtertoday=get_ps(filter_obj.id);
    // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, getPeriodSelect(object.id)))
  }

if (filtertoday=== "1t") {
  filters_arr.push({
    field: "time",
    operator: "between",
    from:timezeroToday,
    to:timestamp,
  });
}
else if(filtertoday=== "2y"){
  filters_arr.push({
    field: "time",
    operator: "between",
    from:timezeroYesterday,
    to:timeendYesterday,
  });
}
else if(filtertoday=== "3dby"){
  filters_arr.push({
    field: "time",
    operator: "between",
    from:timezeroBDYesterday,
    to:timeendBDYesterday,
  });
}
else{
  // filters.push(timeValue(object.tileContext.period, object.tileContext.timeframe, filtertoday))

  if(gf_obj) {
    // filtertoday=getGlobalFilters();
    filters_arr.push(tv_fn(filter_obj.tileContext.period, filter_obj.tileContext.timeframe, get_gf()))
  } else {
    // filtertoday=getPeriodSelect(object.id);
    filters_arr.push(tv_fn(filter_obj.tileContext.period, filter_obj.tileContext.timeframe, get_ps(filter_obj.id)))
  }

}

}
//  -------- Ends Added today tomorrow day before yesterday for download and chart rendering ----------------
