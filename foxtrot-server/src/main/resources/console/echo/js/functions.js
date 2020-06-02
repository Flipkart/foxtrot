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
  $("#sidebar-content").find(el).selectpicker('refresh');
}

function generateTemplateFilter(el, type, fieldList) {
  var selectedColumn = $(el).val();
  var columnType = fieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  console.log(rowIdArray)
  var rowId = rowIdArray[3];
  var el = $("#filter-type-option-"+rowId);
  $("#template-filter-form").find(".template-filter-rows").find(el)
    .find('option')
    .remove()
    .end()
    .append(getTilesFilterWhereOption(type));
    $("#template-filter-form").find(".template-filter-rows").find(el).selectpicker('refresh')
}


function templateFilterFieldTriggered(el) {
  var selectedColumn = $(el).val();
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[3];
  if(selectedColumn == "exists") {
    $('#filter-column-row-'+rowId).hide();
  } else {
    $('#filter-column-row-'+rowId).show();
  }
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

/**
 * Refresh pages without loading
 * @param {*} selectedConsole
 */
function loadConsolesWithoutRefreshing(selectedConsole) {

  stopRefreshInterval();
  getConsoleById(selectedConsole);
  //refereshTiles();
  isNewConsole = false;
  isEdit = false;
  isTemplateFilter = false;
  isViewingVersionConsole = false;
  hideTemplateFilters();
  clearTemplateFilter();

  $('.template-filter-switch').attr('checked', false).triggerHandler('click');
  $('.filter-switch').attr('checked', false).triggerHandler('click');
  globalFilterResetFromConsoleLoad();

  clearForms();

  // fetch selected console id
  // Update broweser URL
  var fullUrl = window.location.href;
  var newUrl = fullUrl.substr(0, fullUrl.indexOf('?'));
  window.history.pushState(null, "Echo", newUrl+"?console="+selectedConsole);


  setTimeout(function () { // triiger version console api
    loadVersionConsoleByName(currentConsoleName);
  }, 5000);

}

// same as globalFilterResetDetails excluding refresh tiles
function globalFilterResetFromConsoleLoad() {
  globalFilters = false;
  hideFilters();
  resetPeriodDropdown();
  resetGloblaDateFilter();
}

function globalFilterResetDetails() {
  globalFilters = false;
  hideFilters();
  resetPeriodDropdown();
  resetGloblaDateFilter();
  refereshTiles();
}

function resetGloblaDateFilter() {
  isGlobalDateFilter = false;
  globalDateFilterValue = "";
  $("#selected-global-date span").text('');
  $("#selected-global-date").hide();
}

function loadParticularConsole() { // reload page based on selected console
  var selectedConsole = $("#listConsole").val();
  if(window.location.href.indexOf("fql") > -1 || window.location.href.indexOf("browse") > -1) {
    window.location.href = "/echo/index.htm?console=" + selectedConsole    
 } else {
   //window.location.assign("index.htm?console=" + selectedConsole);
   loadConsolesWithoutRefreshing(selectedConsole)
 }
}

function getWhereOption(fieldType) {
  var allOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="contains">Equals</option><option value="in">In</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="between">Between</option><option value="exists">Exist</option><option value="not_in">Not In</option>';


  var stringOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="contains">Contains</option><option value="exists">Exist</option><option value="in">In</option><option value="not_in">Not In</option>';

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
  var allOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="contains">Contains</option><option value="between">Between</option><option value="in">In</option><option value="not_in">Not In</option><option value="exists">Exist</option>';

  var stringOption = '<option value="">Select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="contains">Contains</option><option value="in">In</option><option value="not_in">Not In</option><option value="exists">Exist</option>';

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

function getTemplateFilterTable() {
  return $("#template-filter-form").find("#template-filter").val();
}

function queryTypeTriggered(el) {
  var currentFieldList = tableFiledsArray[getTemplateFilterTable()].mappings;
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[3];
  $('#filter-column-row-' + rowId).val('');
  if (columnType == "STRING") {
    $('#filter-column-row-' + rowId).prop("type", "text");
  } else if (columnType == "LONG") {
    $('#filter-column-row-' + rowId).prop("type", "number");
  }
}

function setBetweenInput(el) {
  var selectedType = $(el).val();
  var rowString = $(el).attr('id');

  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[3];
  if (selectedType == "between") {
    $('#filter-between-input-' + rowId).prop("disabled", false);
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".between-element").show();
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".filter-value-div").removeClass('col-sm-6').addClass('col-sm-3');
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".template-filter-value").addClass('template-between-remove-top');
    // $("#template-filter-form").find("#template-filter-row-" + rowId).find(".between-element").addClass("templateFilter-between-value");
  } else {
    $('#filter-between-input-' + rowId).prop("disabled", true);
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".between-element").hide();
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".filter-value-div").removeClass('col-sm-3').addClass('col-sm-6');
    $("#template-filter-form").find("#template-filter-row-" + rowId).find(".template-filter-value").removeClass('template-between-remove-top');
    // $("#template-filter-form").find("#template-filter-row-" + rowId).find(".between-element").removeClass("templateFilter-between-value");
  }
  $('#filter-between-input-' + rowId).val("");
}

function deletTemplateFilterQueryRow(el) {
  var parentRow = $(el).parent();
  var parentRowId = parentRow.attr('id');
  var getRowId = parentRowId.split('-');
  var rowId = getRowId[3];
  var index = templateFilterArray.indexOf(parseInt(rowId));
  templateFilterArray.splice(index, 1);
  $(parentRow).remove();
}

function getTemplateFilters() {
  templateFilterDetails = [];
  var fieldList = tableFiledsArray[getTemplateFilterTable()].mappings;
  for (var filter = 0; filter < templateFilterArray.length; filter++) {
    var filterId = templateFilterArray[filter];
    var el = $(".template-filter-rows").find(".template-row-" + filterId);
    var filterColumn = $(el).find("select.filter-column").val();
    var filterType = $(el).find("select.filter-type").val();
    var filterValue = $(el).find(".template-filter-value").val();
    var filterObject;

    var fieldName;

    if(fieldList[parseInt(filterColumn)]) {
      fieldName = fieldList[parseInt(filterColumn)].field;
    }

    if(fieldName && filterValue) {
      if(filterType == "in" || filterType == "not_in") {
        filterValue = filterValue.split(',');

        var arrayValue = [];
        for(var i = 0; i < filterValue.length; i++) {
          arrayValue.push(filterValue[i].trim());
        }

        filterObject = {
          "operator": filterType
          , "values": arrayValue
          , "field": fieldList[parseInt(filterColumn)].field
        }
      } else if(filterType == "exists") {
        filterObject = {
          "operator": filterType
          , "field": fieldList[parseInt(filterColumn)].field
        }
      } else if(filterType == "between") {
        filterObject = {
          "operator": filterType
          , "field": fieldList[parseInt(filterColumn)].field
          , "from" : $(el).find(".template-filter-between-value").val()
          , "to" : $(el).find(".template-filter-value").val()
        }
      } else {
        filterObject = {
          "operator": filterType,
          "value": filterValue,
          "field": fieldList[parseInt(filterColumn)].field
        }
      }
      templateFilterDetails.push(filterObject);
    }

    if(fieldName) {
      if(filterType == "exists") {
        filterObject = {
          "operator": filterType
          , "field": fieldList[parseInt(filterColumn)].field
        }
        templateFilterDetails.push(filterObject);
      }
    }
  }
  return templateFilterDetails;
}

function renderTemplateFilter(tableName) {
  if(tableName != "none") {
    var fieldList = tableFiledsArray[tableName].mappings;
    var filterCount = templateFilterArray.length;

    if (templateFilterArray.length == 0) {
      templateFilterArray.push(filterCount);
    } else {
      filterCount = templateFilterArray[templateFilterArray.length - 1] + 1;
      templateFilterArray.push(filterCount);
    }

    var filterRow = '<div class="row clearfix template-row-'+filterCount+'" id="template-filter-row-' + filterCount + '"><img src="img/remove.png" class="template-filter-remove-img template-filters-delete" id="' + filterCount + '" /><div class="col-sm-3"><select class="selectpicker form-control filter-column filter-background" id="template-filter-row-' + filterCount + '" data-live-search="true" name="filter-column-' + filterCount + '" required></select></div><div class="col-sm-3"><select required class="filter-type filter-type-option-' + filterCount + ' filter-background form-control" id="filter-type-option-' + filterCount + '"></select></div><div class="col-sm-3 between-element"><input id="filter-between-input-' + filterCount + '" type="number" class="form-control template-filter-between-value  form-control" id="between-value-' + filterCount + '" disabled></div><div class="col-sm-6 template-filter-value-box filter-value-div"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control template-filter-value form-control" name="template-filter-value-' + filterCount + '" required></div></span></div></div>';
    $(".template-filter-rows").append(filterRow);
    var filterValueEl = $("#template-filter-row-" + filterCount).find('.template-filters-delete');
    var filterType = $("#template-filter-row-" + filterCount).find('.filter-type');
    $(filterType).selectpicker('refresh');
    var filterColumn = $("#template-filter-row-" + filterCount).find('.filter-column');
    setTimeout(function () {
      generateDropDown(fieldList, filterColumn);
    }, 0);

    $(filterValueEl).click(function () {
      deletTemplateFilterQueryRow(this);
    });
    $(filterColumn).change(function () {
      var selected = $(this).val();
      if (selected) {
        $(this).next().css("display", "none");
      } else {
        $(this).next().css("display", "block");
      }
      queryTypeTriggered(this);
      generateTemplateFilter(this, fieldList[parseInt(selected)].type, tableFiledsArray[tableName].mappings);
    });
    $(filterType).change(function () {
      setBetweenInput(this);
      templateFilterFieldTriggered(this);
    });
  } else {
    alert("Please select a table");
  }
}

$(".template-filter").change( function(e) {
  fetchTemplateFiltersFields($(this).val());
});


$("#template-filter-add-values").click(function() {
  renderTemplateFilter(getTemplateFilterTable());
});

$("#template-filter-submit-values").click(function() {
  if(templateFilterArray.length > 0) {
    isTemplateFilter = true;
    refereshTiles();
  } else {
    alert(" ADD Template Filters");
  }
});

$(".refresh-widgets").click(function(){
  refereshTiles();
});

function showTemplateFilters() {
  $("#template-filter").show();
}

function hideTemplateFilters() {
  $("#template-filter").hide();
}

function isAppendTemplateFilters(tableName) {
  if(isTemplateFilter) {
    var tempTableName = getTemplateFilterTable();
    if(tableName == tempTableName) {
      return getTemplateFilters();
    } else {
      return [];
    }
  } else {
    return [];
  }
}

function clearTemplateFilter() {
  $("#template-filter").find(".template-filter-rows").empty();
  templateFilterArray = [];
  templateFilterDetails = [];
}

function checkIsRenderTemplateFilter(tableName) {
  if(templateFilterArray.length == 0) {
    renderTemplateFilter(tableName);
  } else {
    clearTemplateFilter();
    renderTemplateFilter(tableName);
  }
}

function fetchTemplateFiltersFields(tableName) { // fetching field for particular table
  $.ajax({
    url: apiUrl+"/v1/tables/" + tableName + "/fields",
    contentType: "application/json",
    context: this,
    success: function(resp){
      tableFiledsArray[tableName] = resp;
      checkIsRenderTemplateFilter(tableName);
    }
  });
}