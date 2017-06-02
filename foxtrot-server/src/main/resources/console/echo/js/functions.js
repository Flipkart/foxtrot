function addTilesList(object) {
  tiles[object.id] = object;
  tileList.push(object.id);
}

function setClicketData(ele) {
  customBtn = ele;
  defaultPlusBtn = false;
  clearModal();
  showHideSideBar();
}

function fetchFields(tableName) {
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
    console.log(uniqueArray[i])
    fetchFields(uniqueArray[i]);
  }
}

function renderTiles(object) {
  var tileFactory = new TileFactory();
  tileFactory.tileObject = object;
  tablesToRender.push(object.tileContext.table);
  tileFactory.create();
}

function getPeriodSelect(tileId) {
  return $("#" + tileId).find(".period-select").val();
}

function getGlobalFilters() {
  return $(".global-filter-period-select").val();
}
function filterTypeTriggered(el) {
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
  $('#filter-column-row-'+rowId).val('');
  if(columnType == "STRING") {
    $('#filter-column-row-'+rowId).prop("type", "text");
  } else if(columnType == "LONG") {
    $('#filter-column-row-'+rowId).prop("type", "number");
  }
}

function addFilters() {

  var filterCount = filterRowArray.length;
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row filters clearfix" id="filter-row-' + filterCount + '"><span class="filter-headings"> FILTER '+(filterCount + 1)+'</span><img src="img/remove.png" class="filter-remove-img filter-delete" /><div class="form-group"><select class="selectpicker form-control filter-column filter-background" id="filter-row-' + filterCount + '" data-live-search="true"><option>select</option></select></div><div class="form-group"><select class="selectpicker filter-type filter-background form-control" data-live-search="true"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option><option value="in">In</option><option value="not_in">Not In</option></select></div><div class="form-group"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control filter-value form-control"></div></span></div></div>';
  $(".add-filter-row").append(filterRow);
  var filterValueEl = $("#filter-row-" + filterCount).find('.filter-delete');
  var filterType = $("#filter-row-" + filterCount).find('.filter-type');
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-" + filterCount).find('.filter-column')
  setTimeout(function(){
    generateDropDown(currentFieldList, filterColumn);
  }, 0);

  $(filterValueEl).click(function () {
    deleteFilterRow(this);
  });
  $(filterColumn).change(function () {
    filterTypeTriggered(this);
  });
}

function showHideForms(currentChartType) {
  $("#table-units").hide();
  console.log(currentChartType);
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

