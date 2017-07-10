var tableNameList = [];
var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
var filterRowArray = [];
var currentFieldList = [];
var tableFiledsArray = {};
var currentTable = "";

function getBrowseTables() {
  var select = $(".browse-table");
  $.ajax({
    url: apiUrl+"/v1/tables/",
    contentType: "application/json",
    context: this,
    success: function(tables) {
      for (var i = tables.length - 1; i >= 0; i--) {
        tableNameList.push(tables[i].name);

      }
      $.each(tableNameList, function(key, value) {
        $(select).append($("<option></option>")
                         .attr("value",value)
                         .text(value));
      });
      select.selectpicker('refresh');
      select.change();
    }});
}

getBrowseTables();

function clear() {
  $(".browse-rows").empty();
  filterRowArray = [];
}

$( ".browse-table" ).change(function() {
  var tableId = this.value;
  fetchFields(tableId);
  currentTable = tableId;
  clear();
});

$( "#browse-events-add-query" ).click(function() {
  console.log(currentTable)
  currentFieldList = tableFiledsArray[currentTable].mappings;
  var filterCount = filterRowArray.length;
  console.log(filterRowArray)
  filterRowArray.push(filterCount);
  var filterRow = '<div class="row clearfix" id="filter-row-' + filterCount + '"><img src="img/remove.png" class="browse-events-filter-remove-img browse-events-delete" /><div class="col-sm-3"><select class="selectpicker form-control filter-column filter-background" id="filter-row-' + filterCount + '" data-live-search="true"><option>select</option></select></div><div class="col-sm-3"><select class="selectpicker filter-type filter-background form-control" data-live-search="true"><option>select</option><option value="between">Between</option><option value="greater_equal">Greater than equals</option><option value="greater_than">Greatert than</option><option value="less_equal">Between</option><option value="less_than">Less than equals</option><option value="less_than">Less than</option><option value="equals">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="last">Last</option><option value="in">In</option><option value="not_in">Not In</option></select></div><div class="col-sm-3"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control browse-events-filter-value form-control"></div><div class="col-sm-3"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control browse-events-filter-between-value form-control" disabled></div></span></div></div>';
  $(".browse-rows").append(filterRow);
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
});

//$( "#browse-events-add-query" ).trigger( "click" );
