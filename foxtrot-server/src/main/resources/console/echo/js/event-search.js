var tableNameList = [];
var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
var browseFilterRowArray = [];
var currentFieldList = [];
var tableFiledsArray = {};
var currentTable = "";
var headerList = [];
var rowList = [];
var selectedList = [];

function getBrowseTables() {
  var select = $(".browse-table");
  $.ajax({
    url: apiUrl + "/v1/tables/"
    , contentType: "application/json"
    , context: this
    , success: function (tables) {
      for (var i = tables.length - 1; i >= 0; i--) {
        tableNameList.push(tables[i].name);

      }
      $.each(tableNameList, function (key, value) {
        $(select).append($("<option></option>")
          .attr("value", value)
          .text(value));
      });
      select.selectpicker('refresh');
      select.change();
    }
  });
}

getBrowseTables();

function clear() {
  $(".browse-rows").empty();
  browseFilterRowArray = [];
}

function deleteBrowseQueryRow(el) {
  var parentRow = $(el).parent();
  var parentRowId = parentRow.attr('id');
  var getRowId = parentRowId.split('-');
  var rowId = getRowId[2];
  var index = browseFilterRowArray.indexOf(parseInt(rowId));
  browseFilterRowArray.splice(index, 1);
  $(parentRow).remove();
}

function queryTypeTriggered(el) {
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
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
  var rowId = parseInt(rowString);
  if (selectedType == "between") {
    $('#filter-between-input-' + rowId).prop("disabled", false);
  } else {
    $('#filter-between-input-' + rowId).prop("disabled", true);
  }
  $('#filter-between-input-' + rowId).val("");
}

$(".browse-table").change(function () {
  var tableId = this.value;
  fetchFields(tableId);
  currentTable = tableId;
  clear();
});

function runQuery() {
  var filters = [];

  for (var filter = 0; filter < browseFilterRowArray.length; filter++) {
    var filterId = browseFilterRowArray[filter];
    var el = $("#filter-row-" + filterId);
    var filterColumn = $(el).find("select.filter-column").val();
    var filterType = $(el).find("select.filter-type").val();
    var filterValue = $(el).find(".browse-events-filter-value").val();
    var filterObject;
    filterObject = {
      "operator": filterType
      , "value": filterValue
      , "field": currentFieldList[parseInt(filterColumn)].field
    }
    filters.push(filterObject);
  }
  var filterSection = $("#browse-events-form");
  var fromDate = $('input[name="daterange"]').daterangepicker.from;
  var toDate = $('input[name="daterange"]').daterangepicker.toDate;
  if ((fromDate - toDate) > 1000) {
    filters.push({
      field: "_timestamp"
      , operator: "between"
      , from: fromDate
      , to: toDate
    });
  }

  var table = currentTable;
  var request = {
    opcode: "query"
    , table: table
    , filters: filters
    , sort: {
      field: "_timestamp"
      , order: filterSection.find("#dataSort").val()
    }
    , from: 0
    , limit: 10
  };
  $.ajax({
    method: 'POST'
    , url: apiUrl + "/v1/analytics"
    , contentType: "application/json"
    , data: JSON.stringify(request)
    , dataType: 'json'
    , success: function (resp) {
      renderTable(resp);
    }
  });
  console.log(request);
}

function generateColumChooserList() {
  var parent = $("#column-chooser");
  var listElement = parent.find("#column-list");
  for (var column in headerList) {
    if (column <= 9) {
      listElement.append("<label><input type='checkbox' value='" + headerList[column] + "' class='column-chooser' checked> &nbsp;&nbsp;&nbsp;" + headerList[column] + "</label><br/>");
      selectedList.push(headerList[column]);
    } else {
      listElement.append("<label><input type='checkbox' value='" + headerList[column] + "' class='column-chooser'> &nbsp;&nbsp;&nbsp;" + headerList[column] + "</label><br/>");
    }
  }

  var selections = []
    , render_selections = function () {
      selections.join(', ');
      selectedList = [];
      selectedList = selections;
    };

  $('.column-chooser').change(function () {
    selections = $.map($('input[type="checkbox"]:checked'), function (a) {
      return a.value;
    })
    render_selections();
  });
}

function renderTable(data) {
  if (!data.hasOwnProperty("documents") || data.documents.length == 0) {
    return;
  }
  var parent = $(".event-display-container");
  var headers = [];
  var headerMap = new Object();
  var rows = [];
  var flatRows = [];

  for (var i = data.documents.length - 1; i >= 0; i--) {
    var flatObject = flat.flatten(data.documents[i]);
    for (field in flatObject) {
      if (flatObject.hasOwnProperty(field)) {
        if (field === "id" || field === "timestamp") {
          continue;
        }
        headerMap[field] = 1;
      }
    }
    flatRows.push(flatObject);
  }
  headers = Object.keys(headerMap);
  for (var i = flatRows.length - 1; i >= 0; i--) {
    var row = [];
    var flatData = flatRows[i];
    for (var j = 0; j < headers.length; j++) {
      var header = headers[j];
      if (flatData.hasOwnProperty(header)) {
        row.push(flatData[header]);
      } else {
        row.push("");
      }
    }
    rows.push(row);
  }
  for (var j = 0; j < headers.length; j++) {
    headers[j] = headers[j].replace("data.", "");
  }
  headerList = headers;
  rowList = rows;
  generateColumChooserList();
  var tableData = {
    headers: headers
    , data: rows
  };
  parent.html(handlebars("#eventbrowser-template", tableData));
}


$("#browse-events-run-query").click(function () {
  runQuery();
});

$("#browse-events-add-query").click(function () {
  currentFieldList = tableFiledsArray[currentTable].mappings;
  var filterCount = browseFilterRowArray.length;

  if (browseFilterRowArray.length == 0) {
    browseFilterRowArray.push(filterCount);
  } else {
    filterCount = browseFilterRowArray[browseFilterRowArray.length - 1] + 1;
    browseFilterRowArray.push(filterCount);
  }

  var filterRow = '<div class="row clearfix" id="filter-row-' + filterCount + '"><img src="img/remove.png" class="browse-events-filter-remove-img browse-events-delete" id="' + filterCount + '" /><div class="col-sm-3"><select class="selectpicker form-control filter-column filter-background" id="filter-row-' + filterCount + '" data-live-search="true"><option>select</option></select></div><div class="col-sm-3"><select class="selectpicker filter-type filter-background form-control" id="' + filterCount + '" data-live-search="true"><option>select</option><option value="equals">Equal to</option><option value="not_equals">Not Equal to</option><option value="less_than">Less than</option><option value="less_equal">Less or equal to</option><option value="greater_than">Greater than</option><option value="greater_equal">Greater or equal to</option><option value="contains">Equals</option><option value="not_equals">Not equals</option><option value="contains">Contains</option><option value="between">Between</option></select></div><div class="col-sm-3"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control browse-events-filter-value form-control"></div><div class="col-sm-3"><input id="filter-between-input-' + filterCount + '" type="text" class="form-control browse-events-filter-between-value form-control" disabled></div></span></div></div>';
  $(".browse-rows").append(filterRow);
  var filterValueEl = $("#filter-row-" + filterCount).find('.browse-events-delete');
  var filterType = $("#filter-row-" + filterCount).find('.filter-type');
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-" + filterCount).find('.filter-column')
  setTimeout(function () {
    generateDropDown(currentFieldList, filterColumn);
  }, 0);

  $(filterValueEl).click(function () {
    deleteBrowseQueryRow(this);
  });
  $(filterColumn).change(function () {
    queryTypeTriggered(this);
  });
  $(filterType).change(function () {
    setBetweenInput(this);
  });
});

function showHideColumnChooser() { // page setting modal
  if ($('#column-chooser').is(':visible')) {
    $('#column-chooser').hide();
  } else {
    $('#column-chooser').show();
    $('#column-chooser').css({
      'width': '356px'
    });
  }
}

//$( "#browse-events-add-query" ).trigger( "click" );
