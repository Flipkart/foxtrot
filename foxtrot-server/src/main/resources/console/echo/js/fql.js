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
    success: function (dataRaw) {
      hideLoader();
      if(dataRaw) {
        renderTable(dataRaw);
        fetchedData = dataRaw;
      } else {
        showErrorAlert('Oops', "No response found");
      }
    },error: function(xhr, textStatus, error) {
      hideLoader();
      if (xhr.hasOwnProperty("responseText")) {
        var error = JSON.parse(xhr["responseText"]);
        if (error.hasOwnProperty('code')) {
          showErrorAlert('Oops', error['code']);
        } else {
          showErrorAlert('Oops', "Something went wrong");
        }
      }
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

// Get query
function saveFqlQuery() {
  showLoader();
  var data = {
    "title": $(".fql-title").val(),
    "query": $(".fql-query").val()
  }
  $.ajax({
    method: 'POST',
    url: apiUrl + "/v1/fql/save",
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function (response) {
      hideLoader();
      if(response) {
        showSuccessAlert('Success', 'FQL is saved sucessfully.');
        $("#save-fql-modal").modal('hide');
      } else {
        showErrorAlert('Oops', "No response found");
      }
    },error: function(xhr, textStatus, error) {
      hideLoader();
      if (xhr.hasOwnProperty("responseText")) {
        var error = JSON.parse(xhr["responseText"]);
        if (error.hasOwnProperty('code')) {
          showErrorAlert('Oops', error['code']);
        } else {
          showErrorAlert('Oops', "Something went wrong");
        }
      }
    }
  });
}

function generateAutoSugest(obj) {
  $('#auto-suggest').empty();
  var list = '';
  $.each(obj, function( key, value ) {
      list +="<li class='list'><label>" + value.query + "</label></li>";
      console.log(value.title);
  })
  $("#auto-suggest").append(list);
  $("#auto-suggest").show();  
}

$("#auto-suggest").on("click", ".list", function(){
  $(".fql-query").val($(this).text())
  $("#auto-suggest").hide();
});

$(".fql-query").keyup(function(){
  var value = $(".fql-query").val();
  if(value.length > 3) {
      var data = {
          "title": value,
      }
      $.ajax({
          method: 'POST',
          url: apiUrl + "/v1/fql/get",
          contentType: 'application/json',
          data: JSON.stringify(data),
          success: function (response) {
          if(response) {
              generateAutoSugest(response);
              console.log(response)
          } else {
              //showErrorAlert('Oops', "No response found");
          }
          },error: function(xhr, textStatus, error) {
          if (xhr.hasOwnProperty("responseText")) {
              var error = JSON.parse(xhr["responseText"]);
              if (error.hasOwnProperty('code')) {
              showErrorAlert('Oops', error['code']);
              } else {
              showErrorAlert('Oops', "Something went wrong");
              }
          }
          }
      });
  }
});