var apiUrl = getHostUrl();
var isEdit = false;
var headerList = [];
var rowList = [];
var selectedList = [];
var fetchedData = [];

function loadConsole() { // load console list api
  $.when(getConsole()).done(function(a1){
    appendConsoleList(a1);
  });
}

loadConsole();

function renderTable(dataRaw) {
  var data = JSON.parse(dataRaw);
  var headerData = data['headers'];
  var headers = []
  for (var i = 0; i < headerData.length; i++) {
    headers.push(headerData[i]['name']);
  }

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

  var rowData = data['rows'];
  var rows = [];
  for (var i = 0; i < rowData.length; i++) {
    var row = []
    for (var j = 0; j < headers.length; j++) {
      row.push(rowData[i][headers[j]]);
    }
    rows.push(row);
  }

  headerList = headers;
  if (!isEdit)
    generateColumChooserList();

  var tableData = {
    headers: headers,
    data: rows
  };
  $(".fql-display-container").html(handlebars("#fql-template", tableData));
}

// Get query
function fqlQuery() {
  showLoader();
  $.ajax({
    method: 'POST',
    url: apiUrl + "/v1/fql",
    data: $(".fql-query").val(),
    dataType: "text",
    accepts: {
      text: 'application/json',
      csv: 'text/csv'
    },
    statusCode: {
      500: function (data) {
        hideLoader();
        if (data.hasOwnProperty("responseText")) {
          var error = JSON.parse(data["responseText"]);
          if (error.hasOwnProperty('message')) {
            showErrorAlert('Oops', error['message']);
          }
        }
      }
    },
    success: function (dataRaw) {
      renderTable(dataRaw);
      fetchedData = dataRaw;
      hideLoader();
    }
  });
}

// Check valid form
function checkValidForm() {
  if (!$("#fql-form").valid()) {
    return false;
  } else {
    return true;
  }
}

$("#fql-run-query").click(function () {
  if (checkValidForm()) {
    fqlQuery();
  }
});

function generateColumChooserList() {
  var parent = $("#fql-column-chooser");
  var listElement = parent.find("#fql-column-list");
  for (var column in headerList) {
    listElement.append("<div class='fql-column-chooser-div'><label><input type='checkbox' checked value='" + headerList[column] + "' class='fql-column-chooser-checkbox'><span class='column-name-text-display'>" + headerList[column] + "</span></label></div>");
    selectedList.push(headerList[column]);
  }

  // Search columns
  $('.fql-search-columns').on('keyup', function () {
    var query = this.value;
    $('[class^="fql-column-chooser-checkbox"]').each(function (i, elem) {
      console.log(elem.value.indexOf(query))
      if (elem.value.indexOf(query) != -1) {
        $(this).closest('label').parent().css({"display": "block"});
      } else {
        $(this).closest('label').parent().css({"display": "none"});
      }
    });
  });

  // get all selected columns
  var selections = [],
    render_selections = function () {
      selectedList = [];
      selectedList = selections;
    };

  $('.fql-column-chooser-checkbox').change(function () {
    selections = $.map($('input[type="checkbox"]:checked'), function (a) {
      return a.value;
    })

    // check select all checkbox check or uncheck
    if ($('.fql-column-chooser-checkbox:checked').length == $('.fql-column-chooser').length) {
      //do something
      $(".fql-select-all").prop('checked', true);
    } else {
      $(".fql-select-all").prop('checked', false);
    }

    render_selections();
  });
}

function showHideColumnChooser() { // page setting modal
  if ($('#fql-column-chooser').is(':visible')) {
    $('#fql-column-chooser').hide();
  } else {
    $('#fql-column-chooser').show();
    $('#fql-column-chooser').css({
      'width': '356px'
    });
  }
}

// Download csv
$("#fql-csv-download").click(function (event) {
  var fqlQueryInput = $(".fql-query");
  var fqlQuery = fqlQueryInput.val();
  if (!checkValidForm()) {
    return;
  }
  window.open(apiUrl + "/v1/fql/download" + "?q=" + encodeURIComponent($(".fql-query").val()), '_blank');
  event.preventDefault();
});
