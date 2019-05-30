function result (rawdata){
    document.getElementById("get-mapping-query").value=rawdata;
}
function rungetquery(dataType, renderFunction) {
    $(".dataview").html("");
    var IndexInput = document.getElementById("get-mapping-query").value;
    $("#wait-dialog-2").modal();
    var hostDetails = new HostDetails("localhost", 9200);
    var path = "http://" + hostDetails.hostname + ":" + hostDetails.port + "/_template/"+IndexInput.toString();
  //  $.get(path,result(rawdata));
    var rawdata = $.ajax({
        method: 'GET',
        url: path,
        async : false,
        contentType : "application/json",
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
      //  success: renderFunction
    }).responseText;

    document.getElementById("get-mapping-query").value=rawdata;
}

$(function () {
    $('[data-toggle="tooltip"]').tooltip();
    $(document).on('submit', 'form', function (event) {
        rungetquery('text', function (dataRaw) {

            document.getElementById("get-mapping-query").value=dataRaw;
            // var data = JSON.parse(dataRaw);
            // var headerData = data['headers'];
            // var headers = []
            // for (var i = 0; i < headerData.length; i++) {
            //     headers.push(headerData[i]['name']);
            // }
            // var rowData = data['rows'];
            // var rows = [];
            // for (var i = 0; i < rowData.length; i++) {
            //     var row = []
            //     for (var j = 0; j < headers.length; j++) {
            //         row.push(rowData[i][headers[j]]);
            //     }
            //     rows.push(row);
            // }
            // var tableData = {headers: headers, data: rows};
            // $(".dataview").html(handlebars("#table-template", tableData));

        });
        event.preventDefault();
    });
})