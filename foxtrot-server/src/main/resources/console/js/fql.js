function fqlError(message) {
    var area = $("<div>", {class: "fql-error-section"});
    area.html(message);
    $(".dataview").html(area);
}

function runFql(dataType, renderFunction) {
    $(".dataview").html("");
    var fqlQueryInput = $(".fql-query");
    var fqlQuery = fqlQueryInput.val();
    if (!fqlQuery) {
        fqlError("Please enter a valid query");
        return;
    }
    var request = {
                query: fqlQuery,
                extrapolationFlag: true
              };
    $("#wait-dialog").modal();
    var hostDetails = new HostDetails("foxtrot.nm.flipkart.com", 80);
    $.ajax({
        method: 'POST',
        url: hostDetails.url("/foxtrot/v2/fql/extrapolation"),
        contentType: "application/json",
        data: JSON.stringify(request),
        dataType: "text",
        accepts: {
            text: 'application/json',
            csv: 'text/csv'
        },
        headers: { 'X-SOURCE-TYPE': 'FQL' },
        statusCode: {
            500: function (data) {
                if (data.hasOwnProperty("responseText")) {
                    var error = JSON.parse(data["responseText"]);
                    if (error.hasOwnProperty('error')) {
                        fqlError(error['error']);
                    }
                }
            }
        },
        success: renderFunction
    }).complete(function () {
        $("#wait-dialog").modal('hide');
    });
}

// Fetching App name from the fql-query
function getAppName() {
    var fql_query = $(".fql-query").val();
    console.log(fql_query);
    if(null == fql_query || fql_query.length==0)
    {
        return getDate();
    }
    var arr = fql_query.replace(fql_query.match(/FROM/i), 'from').split(/from/g);
    var filename = arr[1].trim().split(' ')[0] + '-' + getDate();
    return filename;
}
// Get current date
function getDate() {
    var nowDate = new Date();
    var date = nowDate.getDate()+'-'+(nowDate.getMonth()+1)+'-'+nowDate.getFullYear();
    return date;
}

$(function () {
    //console.log()
    if(isLoggedIn()) {
        $('[data-toggle="tooltip"]').tooltip();
        $(".csv-download").click(function (event) {
            var fqlQueryInput = $(".fql-query");
            var fqlQuery = fqlQueryInput.val();
            if (!fqlQuery) {
                fqlError("Please enter a valid query");
                return;
            }

           var filename = getAppName();
           var hostDetails = new HostDetails("foxtrot.nm.flipkart.com", 80);
        $.ajax({
            method: "get"
            , url: hostDetails.url("/foxtrot/v2/fql/download/") +filename + "?q=" + encodeURIComponent($(".fql-query").val())
            , headers: { 'X-SOURCE-TYPE': 'FQL' }
            , success: (data)=>{
                var blob = new Blob([data], {type: 'application/vnd.ms-excel'});
                var downloadUrl = URL.createObjectURL(blob);
                var a = document.createElement("a");
                a.href = downloadUrl;
                a.download = filename + ".csv";
                document.body.appendChild(a);
                a.click();
            }
        ,error: function(xhr, textStatus, error) {
          showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
        }
      });
      event.preventDefault();
        })
        $(document).on('submit', 'form', function (event) {
            runFql('text', function (dataRaw) {
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
                $(".dataview").html(handlebars("#table-template", tableData));
    
            });
            event.preventDefault();
        });
    }
})