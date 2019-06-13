function addTilesList(object) { // add tiles list to tile list array
  tiles[object.id] = object;
  tileList.push(object.id);
}

function setClicketData(ele) {
  customBtn = ele;
  defaultPlusBtn = false;
  clearModal();
  showHideSideBar();
}

function fetchFields(tableName) { // fetching field for particular table
  $.ajax({
    url: apiUrl+"/v1/tables/" + tableName + "/fields",
    contentType: "application/json",
    context: this,
    success: function(resp){
      tableFiledsArray[tableName] = resp;
    }
  });
}

function fetchTableFields() {
  var uniqueArray = tablesToRender.filter(function(item, pos) {
    return tablesToRender.indexOf(item) == pos;
  });
  for(var i = 0; i < uniqueArray.length; i++) {
    fetchFields(uniqueArray[i]);
  }
}

function renderTiles(object) {
  if(object) {
    var tileFactory = new TileFactory();
    tileFactory.tileObject = object;
    tablesToRender.push(object.tileContext.table);
    tileFactory.create();
  }
}

function getPeriodSelect(tileId) { // period select value for each tiles
  return $("#" + tileId).find(".period-select").val();
}

function getGlobalFilters() { // return global filter values
  return $(".global-filter-period-select").val();
}
function filterTypeTriggered(el) { // changing filter value attribute based on type
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
  $('#filter-column-row-'+rowId).val('');
  if(columnType == "STRING") {
    $('#filter-column-row-'+rowId).prop("type", "text");
    $(".filter-date-picker-"+rowId).hide();
  } else if(columnType == "LONG") {
    $('#filter-column-row-'+rowId).prop("type", "number");
    $(".filter-date-picker-"+rowId).show();
  } else {
    $('#filter-column-row-'+rowId).prop("type", "text"); // for boolean or others type
    $(".filter-date-picker-"+rowId).hide();
  }
}


function generateOption(el, type) {
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
  var optionString = getWhereOption(type);
  var el = $("#filter-type-"+rowId);
  $("#sidebar-content").find(el)
    .find('option')
    .remove()
    .end()
    .append(getTilesFilterWhereOption(type));
  $("#sidebar-content").find(el).selectpicker('refresh')

}

function filterFieldTriggered(el) {
  var selectedColumn = $(el).val();
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
  if(selectedColumn == "exists") {
    $('#filter-column-row-'+rowId).hide();
  } else {
    $('#filter-column-row-'+rowId).show();
  }
}

function setDatePicker(el){
  var selectedColumn = $(el).val();
  var rowString = $(el).attr('class');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[3];
  if(selectedColumn == "true") {
    $("#filter-column-row-div" +rowId).datetimepicker();
    $('#filter-column-row-div'+rowId).datetimepicker({format: 'YYYY-MM-DD hh:mm'});
    $('#filter-column-row-div'+rowId).on("dp.change",function(e) {
       var ts = moment(e.date, "YYYY-MM-DD hh:mm a").valueOf();
       $("#filter-column-row-" + rowId).val(ts);
    });
  } else {
    var tempElement = $("#filter-column-row-div" +rowId);
    $("#filter-column-row-div" +rowId).remove();
    $("#filter-row-"+rowId).append(tempElement);
  }
}

function addFilters() { // new filter row

  var filterCount = filterRowArray.length;
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row filters clearfix" id="filter-row-' + filterCount + '"><span class="filter-headings"> FILTER '+(filterCount + 1)+'</span><img src="img/remove.png" class="filter-remove-img filter-delete" /><div class="form-group"><select class="selectpicker form-control filter-column filter-background" id="filter-row-' + filterCount + '" data-live-search="true"><option>select</option></select></div><div class="form-group"><select class="selectpicker filter-type-' + filterCount + ' filter-background form-control" data-live-search="true" id="filter-type-' + filterCount + '"><option>select</option></select></div><div class="form-group filter-date-picker-radio-btn filter-date-picker-'+filterCount+'"><label id="date-picker-lbl">Date picker?</label><input type="radio" name="enableDatePicker" value="true" class="display-date-picker-' + filterCount + '"><label class="date-picker-radio-text">Yes</label><input type="radio" name="enableDatePicker" value="false" class="display-date-picker-' + filterCount + '"><label class="date-picker-radio-text">No</label></div><div class="form-group filter-date-field input-group date" id="filter-column-row-div' + filterCount + '"><input  id="filter-column-row-' + filterCount + '" type="number" class="form-control filter-value form-control" /><span class="input-group-addon"><span class="glyphicon glyphicon-calendar"></span></span></div></span></div></div>';
  $(".add-filter-row").append(filterRow);
  var filterValueEl = $("#filter-row-" + filterCount).find('.filter-delete');
  var filterType = $("#filter-row-" + filterCount).find('.filter-type-'+ filterCount);
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-" + filterCount).find('.filter-column')
  setTimeout(function(){
    generateFiltersDropDown(currentFieldList, filterColumn);
  }, 0);

  $(filterValueEl).click(function () {
    deleteFilterRow(this);
  });
  $(filterColumn).change(function () {
    var selected = $(this).val();
    filterTypeTriggered(this);
    generateOption(this, currentFieldList[parseInt(selected)].type);
  });
  $(filterType).change(function () {
    var selected = $(this).val();
    filterFieldTriggered(this);
  });

  var enableDatePicker = $("#filter-row-" + filterCount).find('.display-date-picker-'+ filterCount);
  $(enableDatePicker).click(function(){
    setDatePicker(this);
  });

}

function showHideForms(currentChartType) { // remove active class for widget forms
  $("#table-units").hide();
  $("#table-units").find("#"+currentChartType).find(".table-units-active").removeClass(".table-units-active");
}

function removeFilters() {
  $(".filters").remove();
  filterRowArray = [];
}

function clearModal() {
  $("#widgetType").val('');
  $("#tileTitle").val('');
  $(".tile-table").val('');
  $('.tile-table option').last().prop('selected', true);
  $(".tile-table").selectpicker('refresh');
  $(".tile-table").change();
  $("#tileTimeFrame").val('');
  $(".tile-time-unit").val('minutes');
  $(".sidebar-tileId").val('');
  $(".vizualization-type").show();
  $(".vizualization-type").removeClass("vizualization-type-active");
  removeFilters();
  $("#table-units").hide();
  $(".chart-type").show();
  $('.chart-type option').first().prop('selected', true);
  $("#tile-description").val('');
}

function hideFilters() {
  $(".global-filters").removeClass('col-sm-3');
  $(".global-filters").addClass('col-sm-2');
  $(".global-filters").css({'width': "138px"});
  $(".global-filter-switch-div").css({'border': "none"})
  $(".widget-btns").css({'left': "172px"});
  $(".hide-filters").css({"display": "none"});
}

function showFilters() {
  $(".global-filters").removeClass('col-sm-2');
  $(".global-filters").addClass('col-sm-3');
  $(".global-filter-switch-div").css({'border': "none"})
  $(".widget-btns").css({'left': "0px"});
  $(".hide-filters").css({"display": "block"});
  $(".global-filter-switch-div").css({'border-right': "1px solid #aeb8bd"});
  $(".global-filters").css({'width': "auto"});
}

function hideConsoleModal(id) {
  $("#"+id).modal('hide');
}
function getActiveTabIndex() {
  var activeIndex = $('.tab button.active').index();
  return parseInt(activeIndex);
}

function sideBarScrollTop() {
  $("#sidebar-content").animate({ scrollTop: 0 }, "fast");
}

function sortTiles(tileData) {
  return Object.keys(tileData).sort(function (x, y) {
    var n = tileData[x].tileContext.row - tileData[y].tileContext.row;
    if (n !== 0) {
      return n;
    }
    return tileData[x].tileContext.position - tileData[y].tileContext.position;
  });
}

function getFilterCheckBox() {
  var allVals = [];
  $('input[name=filter-checkbox]:checked').each(function() {
    allVals.push($(this).val());
  });
  return allVals;
}

function showSelectAllAction() {
  $("#select-all-checkbox").removeClass('show');  
  $("#select-all-checkbox").addClass('hide');
  $("#un-select-all-checkbox").removeClass('hide');
  $("#un-select-all-checkbox").removeClass('show');
}

function showUnselectAllAction() {
  $("#un-select-all-checkbox").removeClass('show');
  $("#un-select-all-checkbox").addClass('hide');  
  $("#slect-all-checkbox").removeClass('hide');
  $("#select-all-checkbox").addClass('show');
}

function selectAllUiCheckbox() {
  $(".ui-filter-checkbox").prop('checked', 'true');
  showSelectAllAction();
}

function unSelectAllUiCheckbox() {
  $(".ui-filter-checkbox").removeAttr('checked');
  showUnselectAllAction();
}

// listen and enable/disable check/uncheck button
function listenUiFilterCheck() {
  var totalCount = $('.ui-filter-checkbox').length;
  var totalCheckedLegnth = $('.ui-filter-checkbox:checkbox:checked').length;
  if(totalCount == totalCheckedLegnth) {
    showSelectAllAction();
  } else {
    showUnselectAllAction();
  }
}

// sort by name
function sortConsoleArray(array) {
   return array.sort(function(a, b){ // sort by name
    var nameA=a.name.toLowerCase(), nameB=b.name.toLowerCase();
    if (nameA < nameB) //sort string ascending
      return -1;
    if (nameA > nameB)
      return 1;
    return 0; //default return value (no sorting)
  });
}

function prepareListOption(array, appendVersion) {
  console.log(appendVersion)
  var textToInsert = [];
  var i = 0;
  array = sortConsoleArray(array);
  for (var a = 0; a < array.length; a += 1) {
    var versionString = " - v"+array[a].version;
    textToInsert[i++] = '<option value=' + array[a].id + '>';
    textToInsert[i++] = array[a].name+(appendVersion == true ? versionString : '');
    textToInsert[i++] = '</option>';
  }
  return textToInsert;
}

function appendConsoleList(array) { // console list to dropdown
  $("#listConsole").append(prepareListOption(array, false).join(''));
}

function appendVersionConsoleList(array) {
  $("#version-list").find('option').not(':first').remove();// remove all except first 
  $("#version-list").append(prepareListOption(array, true).join(''));
}

function loadParticularConsole() { // reload page based on selected console
  var selectedConsole = $("#listConsole").val();
  if(window.location.href.indexOf("fql") > -1) {
    window.location.href = "/echo/index.htm?console=" + selectedConsole    
 } else {
    window.location.assign("index.htm?console=" + selectedConsole);
 }
}

function getWhereOption(fieldType) {
  var allOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="contains">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="between">Between</option><option value="exists">Exist</option><option value="not_in">Not In</option>';


  var stringOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="contains">Contains</option><option value="exists">Exist</option><option value="not_in">Not In</option>';

  var boolOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="exists">Exist</option>';

  var intOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="between">Between</option><option value="exists">Exist</option>';

  var intArray = ["LONG", "INTEGER", "SHORT", "BYTE", "DATE", "FLOAT", "DOUBLE"];
  var boolArray = ["BOOLEAN"];
  var stringArray = ["STRING"];

  if(intArray.indexOf(fieldType) > -1) {
    return intOption;

  } else if(boolArray.indexOf(fieldType) > -1) {
    return boolOption;

  } else if(stringArray.indexOf(fieldType) > -1) {
    return stringOption;
  } else {
    return allOption;
  }
}

function getTilesFilterWhereOption(fieldType) {
  var allOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="contains">Contains</option><option value="between">Between</option><option value="last">Last</option><option value="in">In</option><option value="not_in">Not In</option><option value="exists">Exist</option>';

  var stringOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="contains">Contains</option><option value="in">In</option><option value="not_in">Not In</option><option value="last">Last</option><option value="exists">Exist</option>';

  var boolOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="exists">Exist</option>';

  var intOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="between">Between</option><option value="exists">Exist</option>';

  var intArray = ["LONG", "INTEGER", "SHORT", "BYTE", "DATE", "FLOAT", "DOUBLE"];
  var boolArray = ["BOOLEAN"];
  var stringArray = ["STRING"];

  if(intArray.indexOf(fieldType) > -1) {
    return intOption;

  } else if(boolArray.indexOf(fieldType) > -1) {
    return boolOption;

  } else if(stringArray.indexOf(fieldType) > -1) {
    return stringOption;
  } else {
    return allOption;
  }
}

function showLoader() {
  $(".loading").show();
}

function hideLoader() {
  $(".loading").hide();
}

function showErrorAlert(title, msg) {
  swal(
    title,
    msg,
    'error'
  );
}

function showSuccessAlert(title, msg) {
  swal(
    title,
    msg,
    'success'
  );
}

function showInfoAlert(title, msg) {
  swal(
    title,
    msg,
    'info'
  );
}

function getConsole() {
  return $.ajax({
    url: apiUrl+("/v2/consoles/"),
    type: 'GET',
    contentType: 'application/json',
    success: function(res) {
      return res;
    },
    error: function() {
      showErrorAlert("Oops", "Could not get console details");
    }
  });
}

$("#listConsole").change(function () {
  loadParticularConsole();
});

$("#add-sections").click(function () {
  window.location = "index.htm?openDashboard=true";
});

$("#fql-dashboard").click(function () {
  window.location = "../index.htm?openDashboard=true";
});

function getRefreshTime() {
  return 6000; // 6 seconds
}

function getRefreshTimeMultiplyeFactor(str) {
  if(str.endsWith('s')) {
    return 1000; // seconds
  } else if(str.endsWith('m')) {
    return 60 * 1000; // minutes
  }else if(str.endsWith('h')) {
    return 60 * 60 * 1000; // hours
  }else if(str.endsWith('d')) {
    return 1000 * 60 * 60 * 24; // days
  }
}

function getNumberFromString(thestring) {
  return parseInt(thestring.replace( /^\D+/g, ''));
}

/**
 * 
 * show refresh failed msg
 */
function showFetchError(data, errorType, err) {
  var el = $("#"+data.id).find(".fetch-error");
  $(el).text(getErrorMsg(errorType, err));
  $(el).show();

  if(data.tileContext.chartType == "sunburst") { // only for sun burst
    var chartItem = $("#"+data.id).find(".chart-item")
    $(chartItem).hide();
  }
  
  var widgetType = data.tileContext.widgetType;
  if(widgetType == "medium") {
    $(el).addClass('fetch-error-medium-widget');
  } else if(widgetType == "full") {
    $(el).addClass('fetch-error-full-widget');
  } else if(widgetType == "small") {
    $(el).addClass('fetch-error-small-widget');
  }
}

/**
 * 
 * Hide refresh failed msg
 */
function hideFetchError(data) {
  $("#"+data.id).find(".fetch-error").hide();
} 

/**
 * Convert epoch to readble date
 */
function readbleDate(epochValue) {
  var day = moment(epochValue); //milliseconds
  return day.format('DD/MM/YYYY, hh:mm:ss a');
}
/**
 * Convert epoch to readble date d/m/y
 */
function readableShortDate(epochValue) {
  var day = moment(epochValue); //milliseconds
  return day.format('DD/MM/YYYY');
}

/**
 * Check string has special characters
 */
function isSpecialCharacter(string) {
  var format = /(?=[-+*\/])/;
  
  if(format.test(string)){
    return true;
  } else {
    return false;
  }
}

/**
 * Split string by every artimetic operator
 */
function splitArithmetic(arithmetic) {
  return arithmetic.split(/(?=[-+*\/])/)
}

/**
 * Get opcode
 */
function getOpcode(object) {
  if(object.tileContext.chartType == "stackedBar" || object.tileContext.chartType == "nonStackedLine")
    return "trend";
  else if(object.tileContext.chartType == "statstrend")
    return "statstrend";
}

/**
 * prepare multi series query data
 */
function prepareMultiSeriesQueryObject(data, object, filters) {

  //console.log(object)

  var currentTime = filters[0].currentTime;
  var duration = filters[0].duration;
  var period = data.period;
  var loopValue = object.tileContext.multiSeries;
  var durationInNumbers = duration.split(/([0-9]+)/)[1];// seperate string and number
    
  var mapDetails = {};
  for( var i = 0; i < loopValue; i++) {
    var tmpObj = JSON.parse(JSON.stringify(data));
    tmpObj.opcode = getOpcode(object);
    if(period == "days") {
      tmpObj.filters[0].currentTime = moment().subtract(durationInNumbers * i, "days").valueOf();
    } else if(period == "hours") {
      tmpObj.filters[0].currentTime = moment().subtract(durationInNumbers * i, "hours").valueOf();
    } else if(period == "minutes") {
      tmpObj.filters[0].currentTime = moment().subtract( durationInNumbers * i, "minutes").valueOf();
    }
    mapDetails[i+1] = tmpObj;
  }

  return mapDetails;
}

/**
 * Error msgs
 */
function getErrorMsg(errorType, err) {
  if(errorType == "refresh") {
    var errorMsg = (err["error"] == undefined ? err["code"] : err["error"])
    return (errorMsg.length == 0 ? "Refresh failed" : (errorMsg instanceof Array ? errorMsg.join(" ; ") : errorMsg));
  } else if(errorType == "data") {
    return "No results found";
  }
}

/**
 * seperate number and string
 */
function seperateStringAndNumber(inputText) {
  var output = [];
  var json = inputText.split(' ');
  json.forEach(function (item) {
      output.push(item.replace(/\'/g, '').split(/(\d+)/).filter(Boolean));
  });
  return output[0];
}

function getPeriodText(text) {
  if (!text) {
    return null;
  }
  if (text == "d") {
    return "days";
  }
  else if (text == "h") {
    return "hours";
  }
  else {
    return "minutes";
  }
}

/**
 * Get old console list
 */
function getOldConsoleList(res) {
  var consoleId = getParameterByName("console").replace('/', '');
  var index = _.indexOf(_.pluck(res, 'id'), consoleId);
  if (index >= 0) {
      var consoleObject = consoleList[index];
      var numberOfVerison = consoleObject.version;
      if (numberOfVerison > 0) {
          loadVersionConsoleByName(consoleObject.name);
      }
  }
}

/**
 * function to reset broswer url without tab details for older versions list
 */
function resetBrowserUrl() {
  window.history.pushState({}, document.title, window.location.pathname+"?console="+getParameterByName("console").replace('/',''));
}