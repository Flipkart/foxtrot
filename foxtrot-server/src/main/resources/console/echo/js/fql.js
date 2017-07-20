var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
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
      var tableData = {headers: headers, data: rows};
      console.log(rows);
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
