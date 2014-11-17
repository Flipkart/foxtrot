function fqlError(message) {
    var area = $("<div>", {class: "fql-error-section"});
    area.html(message);
    $(".dataview").html(area);
}

function runFql() {
    $(".dataview").html("");
    var fqlQueryInput = $(".fql-query");
    var fqlQuery = fqlQueryInput.val();
    if(!fqlQuery) {
        fqlError("Please enter a valid query"); //TODO::ALERT PROPERLY
        return;
    }
    $("#wait-dialog").modal();
    var hostDetails = new HostDetails("foxtrot.nm.flipkart.com", 80);
    $.ajax({
        method: 'POST',
        url: hostDetails.url("/foxtrot/v1/fql"),
        data: fqlQuery,
        dataType: 'text',
        accepts: {
            text: 'application/json'
        },
        statusCode: {
            500: function(data) {
                if(data.hasOwnProperty("responseText")) {
                    var error = JSON.parse(data["responseText"]);
                    if(error.hasOwnProperty('error')) {
                        fqlError(error['error']);
                    }
                }
            }
        },
        success: function(dataRaw) {
            var data = JSON.parse(dataRaw);
            var headerData = data['headers'];
            var headers = []
            for(var i = 0; i < headerData.length; i++) {
                headers.push(headerData[i]['name']);
            }
            var rowData = data['rows'];
            var rows = [];
            for(var i = 0; i < rowData.length; i++) {
                var row = []
                for(var j = 0; j < headers.length; j++) {
                    row.push(rowData[i][headers[j]]);
                }
                rows.push(row);
            }
            var tableData = {headers : headers, data: rows};
            $(".dataview").html(handlebars("#table-template", tableData));

        }
    }).complete(function() {
        $("#wait-dialog").modal('hide');
    });
}

$(function(){
    $(document).on('submit', 'form', function(event) {
                                      runFql();
                                      event.preventDefault();
                                  });
})