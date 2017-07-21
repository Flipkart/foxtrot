var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
var isEdit = false;
var headerList = [];
var rowList = [];
var selectedList = [];


function loadConsole() { // load console list api
  $.ajax({
    url: apiUrl+("/v2/consoles/"),
    type: 'GET',
    contentType: 'application/json',
    success: function(res) {
      appendConsoleList(res);
    },
    error: function() {
      error("Could not save console");
    }
  })
}

$("#listConsole").change(function () {
  loadParticularConsole();
});

loadConsole();

$("#add-sections").click(function () {
  window.location = "index.htm?openDashboard=true";
});

function fqlQuery() {
  $.ajax({
    method: 'POST',
    url: apiUrl+"/v1/fql",
    data: $(".fql-query").val(),
    dataType: "text",
    accepts: {
      text: 'application/json',
      csv: 'text/csv'
    },
    success: function(dataRaw) {
      var data = JSON.parse(dataRaw);
      var headerData = data['headers'];
      var headers = []
      for (var i = 0; i < headerData.length; i++) {
        headers.push(headerData[i]['name']);
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

      var tableData = {headers: headers, data: rows};
      $(".fql-display-container").html(handlebars("#fql-template", tableData));
    }
  });
}


$("#fql-run-query").click(function () {
  if (!$("#fql-form").valid()) {
    return;
  }
  fqlQuery();
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
      if (elem.value.indexOf(query) != -1) {
        $(this).closest('label').show();
      } else {
        $(this).closest('label').hide();
      }
    });
  });

  // get all selected columns
  var selections = [],
      render_selections = function () {
        selectedList = [];
        selectedList = selections;
      };

  $('.column-chooser').change(function () {
    selections = $.map($('input[type="checkbox"]:checked'), function (a) {
      return a.value;
    })

    // check select all checkbox check or uncheck
    if ($('.column-chooser:checked').length == $('.column-chooser').length) {
      //do something
      $(".select-all").prop('checked', true);
    } else {
      $(".select-all").prop('checked', false);
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
