function fqlError(message) {
    var area = $("<div>", {class: "fql-error-section"});
    area.html(message);
    $(".dataview").html(area);
}

function runmap(dataType, renderFunction) {
 //   $(".dataview").html("");
  //  var fqlQueryInput = $(".fql-query");

   // var fqlQueryInput = '{"name":"text"}';

    var queryinput = document.getElementById("query").value;

    var indexinput = document.getElementById("mapping-query").value;

    var s = JSON.parse(queryinput);

    var query="";

    for (var key in s) {

        query=query+"\""+ key.toString() +"\"" + ": {\n" +
            "  \"type\":" +"\"" +s[key] +"\"" + "}," ;

    }
    var mappingquery = "{ \"index_patterns\": [\n" +
        "    \"foxtrot-"+indexinput+"-*\"\n" +
        "  ]," ;

    var newquery = query.substring(0, query.length-1);

    var prefix = "\"mappings\": {\n" +
        "    \"_doc\": {\n" +
        "      \"properties\": {";


    var postfix = "}\n" +
        "    }\n" +
        "}\n " +
        "}";
    mappingquery = mappingquery + prefix + newquery + postfix ;
    var actualquery = JSON.parse(mappingquery);

  /*  $("#wait-dialog-1").modal();
    var hostDetails = new HostDetails( "localhost" , 9200);
    $.ajax({

        method: 'PUT',
        url: hostDetails.url("/_template/"+indexinput.toString()),
       data: mappingquery,
        contentType: "application/json",

    }).complete(function () {
        $("#wait-dialog-1").modal('hide');
    });
*/



    $("#wait-dialog-1").modal();
    var localhostDetails = new HostDetails( "localhost" , 9200);
   // window.hostDetails.port=9200;
    var abc = localhostDetails.port;
    var path = "http://" + localhostDetails.hostname + ":" + localhostDetails.port + "/_template/"+indexinput.toString();
    $.ajax({

        method: 'PUT',
        url: path,
        data: mappingquery,
        contentType : "application/json",
         datatype: "json"
    }).complete(function () {
        $("#wait-dialog-1").modal('hide');
    });
}

$(function () {
    $('[data-toggle="tooltip"]').tooltip();
  /*  $(".csv-download").click(function (event) {
        $(".dataview").html("");
        var fqlQueryInput = $(".fql-query");
    //    var fqlQuery = fqlQueryInput.val();
    //    if (!fqlQuery) {
    //        fqlError("Please enter a valid query");
    //        return;
    //    }

   //     var hostDetails = new HostDetails("foxtrot.nm.flipkart.com", 80);
    //    window.open(hostDetails.url("/foxtrot/v1/fql/download") + "?q=" + encodeURIComponent($(".fql-query").val()), '_blank');
   //     event.preventDefault();
    }) */
    $(document).on('submit', 'form', function (event) {
        runmap("json", function (dataRaw) {
         /*   var data = JSON.parse(dataRaw);
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
        */

        event.print("Success");

        });
        event.preventDefault();
    });
})