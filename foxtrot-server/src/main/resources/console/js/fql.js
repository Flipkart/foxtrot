function runFql() {
    var fqlQueryInput = $(".fql-query");
    var fqlQuery = fqlQueryInput.val();
    if(!fqlQuery) {
        alert("Please enter a valid query"); //TODO::ALERT PROPERLY
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