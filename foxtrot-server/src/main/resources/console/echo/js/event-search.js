var tableNameList = [];
var apiUrl = getHostUrl();
var browseFilterRowArray = [];
var currentFieldList = [];
var tableFiledsArray = {};
var currentTable = "";
var headerList = [];
var rowList = [];
var selectedList = [];
var fetchedData = [];
var isEdit = false;
var fromDate = 0;
var toDate = 0
var offset = 0;
var isLoadMoreClicked = false;
var didScroll = false;
var totalHits = 0;

function getBrowseTables() {
  var select = $(".browse-table");
  $.ajax({
    url: apiUrl + "/v1/tables/",
    contentType: "application/json",
    context: this,
    success: function (tables) {
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
  $("#column-chooser").find("#column-list").empty();
  $(".event-display-container").find('.event-table').remove();
  browseFilterRowArray = [];
  selectedList = [];
  fetchedData = [];
  isEdit = false;
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

  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[3];
  if (selectedType == "between") {
    $('#filter-between-input-' + rowId).prop("disabled", false);
    $("#browse-events-form").find("#filter-row-" + rowId).find(".between-element").show();
    $("#browse-events-form").find("#filter-row-" + rowId).find(".filter-value-div").removeClass('col-sm-6').addClass('col-sm-3');
    $("#browse-events-form").find("#filter-row-" + rowId).find(".browse-events-filter-value").addClass('browse-remove-top');
  } else {
    $('#filter-between-input-' + rowId).prop("disabled", true);
    $("#browse-events-form").find("#filter-row-" + rowId).find(".between-element").hide();
    $("#browse-events-form").find("#filter-row-" + rowId).find(".filter-value-div").removeClass('col-sm-3').addClass('col-sm-6');
    $("#browse-events-form").find("#filter-row-" + rowId).find(".browse-events-filter-value").removeClass('browse-remove-top');
  }
  $('#filter-between-input-' + rowId).val("");
}

$(".browse-table").change(function () {
  var tableId = this.value;
  fetchFields(tableId);
  currentTable = tableId;
  clear();
});

function runQuery(isBrowse) {
  var filters = [];

  if (isBrowse) {
    offset = 0;
    fetchedData = [];
  }

  for (var filter = 0; filter < browseFilterRowArray.length; filter++) {
    var filterId = browseFilterRowArray[filter];
    var el = $("#filter-row-" + filterId);
    var filterColumn = $(el).find("select.filter-column").val();
    var filterType = $(el).find("select.filter-type").val();
    var filterValue = $(el).find(".browse-events-filter-value").val();
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
        "operator": filterType,
        "value": filterValue,
        "field": currentFieldList[parseInt(filterColumn)].field
      }
    }
    filters.push(filterObject);
  }
  var filterSection = $("#browse-events-form");
  if ((fromDate - toDate) > 1000) {
    filters.push({
      field: "_timestamp",
      operator: "between",
      from: fromDate,
      to: toDate
    });
  }

  var sortValue = filterSection.find("#dataSort").val();
  var sortObject = {
    field: "_timestamp"
  };

  if (sortValue != "none") {
    sortObject.order = sortValue;
  }

  var table = currentTable;
  var request = {
    opcode: "query",
    table: table,
    filters: filters,
    sort: sortObject,
    from: offset,
    limit: 10
  };
  showLoader();
  $.ajax({
    method: 'POST',
    url: apiUrl + "/v1/analytics",
    contentType: "application/json",
    data: JSON.stringify(request),
    dataType: 'json',
    success: function (resp) {
      hideLoader();
      totalHits = resp.totalHits;

      if(totalHits == 0) {
        showErrorAlert('Oops', "No results found");
        return;
      }

      if (fetchedData.documents) {
        var tmpData = fetchedData.documents;
        tmpData.push.apply(tmpData, resp.documents);
        fetchedData.documents = tmpData;
        renderTable(fetchedData);
      } else {
        renderTable(resp);
        fetchedData = resp;
      }
    }, error: function (jqXHR, exception) {
      hideLoader();
      showErrorAlert('Oops', "Something Went wrong, Please try again/Check your inputs");
    }
  });
}

function loadMore() {
  offset = fetchedData.documents.length;
  runQuery(false);
}

function generateColumChooserList() {
  var parent = $("#column-chooser");
  var listElement = parent.find("#column-list");
  for (var column in headerList) {
    if($.inArray(headerList[column], selectedList) == -1) {
      listElement.append("<div class='column-chooser-div'><label><input type='checkbox' checked value='" + headerList[column] + "' class='column-chooser-checkbox'><span class='column-name-text-display'>" + headerList[column] + "</span></label></div>");
      selectedList.push(headerList[column])
    }
  }

  // Search columns
  $('.search-columns').on('keyup', function () {
    var query = this.value;
    $('[class^="column-chooser-checkbox"]').each(function (i, elem) {
      if (elem.value.indexOf(query) != -1) {
        $(this).closest('label').parent().show();
      } else {
        $(this).closest('label').parent().hide();
      }
    });
  });

  // get all selected columns
  var selections = [],
    render_selections = function () {
      selectedList = [];
      selectedList = selections;
    };

  $('.column-chooser-checkbox').change(function () {
    selections = $.map($('input[type="checkbox"]:checked'), function (a) {
      return a.value;
    })

    // check select all checkbox check or uncheck
    if ($('.column-chooser-checkbox:checked').length == $('.column-chooser-checkbox').length) {
      //do something
      $(".select-all").prop('checked', true);
    } else {
      $(".select-all").prop('checked', false);
    }
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

  if (isEdit) {
    var tmpHeaders = [];
    for (column in headers) {
      var header = headers[column];
      var tempHeader = header.replace('data.', '');
      if (selectedList.indexOf(tempHeader) != -1) {
        tmpHeaders.push(header);
      }
    }
    headers = [];
    headers = tmpHeaders;
  }

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
    headers: headers,
    data: rows
  };
  parent.html(handlebars("#eventbrowser-template", tableData));

  $('.event-table').DataTable({
    scrollX: true,
    scrollCollapse: true,
    paging: false,
    info: false,
    bFilter: false,
    fixedColumns: {
      leftColumns: 2
    }
  });

  if (offset == 0)
    document.getElementById("scroll-ref").scrollIntoView();
}


$("#browse-events-run-query").click(function () {
  if (!$("#browse-events-form").valid()) {
    return;
  }
  runQuery(true);
});

function generateOption(el, type) {
  var selectedColumn = $(el).val();
  var columnType = currentFieldList[selectedColumn].type;
  var rowString = $(el).attr('id');
  var rowIdArray = rowString.split('-');
  var rowId = rowIdArray[2];
  var optionString = getWhereOption(type);
  var el = $("#filter-type-option-" + rowId);

  $("#browse-events-form").find(el)
    .find('option')
    .remove()
    .end()
    .append(getWhereOption(type));
    setTimeout(function(){
      $("#browse-events-form").find(el).val('equals');
      $("#browse-events-form").find(el).selectpicker('refresh');
    }, 0);
}


$("#browse-events-add-query").click(function () {
  currentFieldList = tableFiledsArray[currentTable].mappings;
  var filterCount = browseFilterRowArray.length;

  if (browseFilterRowArray.length == 0) {
    browseFilterRowArray.push(filterCount);
  } else {
    filterCount = browseFilterRowArray[browseFilterRowArray.length - 1] + 1;
    browseFilterRowArray.push(filterCount);
  }

  var filterRow = '<div class="row clearfix" id="filter-row-' + filterCount + '"><img src="img/remove.png" class="browse-events-filter-remove-img browse-events-delete" id="' + filterCount + '" /><div class="col-sm-3"><select class="selectpicker form-control filter-column filter-background" id="filter-row-' + filterCount + '" data-live-search="true" name="filter-column-' + filterCount + '" required></select></div><div class="col-sm-3"><select required class="filter-type filter-type-option-' + filterCount + ' filter-background form-control" id="filter-type-option-' + filterCount + '"></select></div><div class="col-sm-6 filter-value-div"><input id="filter-column-row-' + filterCount + '" type="text" class="form-control browse-events-filter-value form-control" name="browse-events-filter-value-' + filterCount + '" required></div><div class="col-sm-3 between-element"><input id="filter-between-input-' + filterCount + '" type="number" class="form-control browse-events-filter-between-value  form-control" id="between-value-' + filterCount + '" disabled></div></span></div></div>';
  $(".browse-rows").append(filterRow);
  var filterValueEl = $("#filter-row-" + filterCount).find('.browse-events-delete');
  var filterType = $("#filter-row-" + filterCount).find('.filter-type');
  $(filterType).selectpicker('refresh');
  var filterColumn = $("#filter-row-" + filterCount).find('.filter-column');
  setTimeout(function () {
    generateDropDown(currentFieldList, filterColumn);
  }, 0);

  $(filterValueEl).click(function () {
    deleteBrowseQueryRow(this);
  });
  $(filterColumn).change(function () {
    var selected = $(this).val();
    if (selected) {
      $(this).next().css("display", "none");
    } else {
      $(this).next().css("display", "block");
    }
    queryTypeTriggered(this);
    generateOption(this, currentFieldList[parseInt(selected)].type);
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

$("#show-more").click(function () {
  $("#more-fields").toggle();
  if (this.text == "Show more") {
    this.text = "Show Less";
  } else {
    this.text = "Show more";
  }
});

function fetchAuto() {
  if (didScroll && fetchedData.documents) {
    offset = fetchedData.documents.length;
    if (offset <= 30) {
      isEdit = true;
      runQuery(false);
    }
    didScroll = false;
  }
}

// $('.container-full').scroll( function(){
//   if($(this).scrollTop() + $(this).height() == $(this)[0].scrollHeight){
//     didScroll = true;
//     if(totalHits > 0)
//       fetchAuto();
//   }
// });


function loadConsole() { // load console list api
  $.when(getConsole()).done(function(a1){
    appendConsoleList(a1);
  });
}

loadConsole();
