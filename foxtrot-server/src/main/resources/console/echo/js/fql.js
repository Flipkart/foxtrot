var apiUrl = getHostUrl();
var isEdit = false;
var headerList = [];
var rowList = [];
var selectedList = [];
var fetchedData = [];
var AutoCallApiStartIndex = 20;
var savedFqlQuery = [];
var saveFqlQuerys = [];


function loadConsole() { // load console list api
    $.when(getConsole()).done(function (a1) {
        appendConsoleList(a1);
       triggerAPI(); 
    });
}

if(isLoggedIn()) {
    loadConsole();     
}  

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
        row.forEach((element ,index) => {
            if(typeof(element) == 'boolean')
            row[index] = element.toString();   
         });
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
    if(isLoggedIn()) {
        showLoader();
        let query = $(".fql-query").val().replace(/^\s+|\s+$/g, "").replace(/\s+/g, " ");
        if(query[query.length - 1] === '.') {
            query = query.slice(query.lenght, -1);
        }
        const extrapolation = $('#mark-extrapolation').is(":checked");
        var request = {
            query: query,
            extrapolationFlag: extrapolation
          };
        $.ajax({
            method: 'POST',
            url: apiUrl + "/v2/fql/extrapolation",
            contentType: "application/json",
            data: JSON.stringify(request),
            dataType: "text",
            accepts: {
                text: 'application/json',
                csv: 'text/csv'
            },
            headers: { 'X-SOURCE-TYPE': 'FQL' },
            success: function (dataRaw) {
                hideLoader();
                if (dataRaw) {
                    renderTable(dataRaw);
                    fetchedData = dataRaw;
                } else {
                    showErrorAlert('Oops', "No response found");
                }
            },
            error: function (xhr, textStatus, error) {
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
            if (elem.value.indexOf(query) != -1) {
                $(this).closest('label').parent().css({
                    "display": "block"
                });
            } else {
                $(this).closest('label').parent().css({
                    "display": "none"
                });
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
    let query = $(".fql-query").val().replace(/^\s+|\s+$/g, "").replace(/\s+/g, " ");
        if(query[query.length - 1] === '.') {
            query = query.slice(query.lenght, -1);
        }
    var filename = getAppName();
    $.ajax({
        method: "get"
        , url: apiUrl + "/v2/fql/download/" + filename + "?q=" + encodeURIComponent(query)
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
});

// Fetching App name from the fql-query
function getAppName() {
    var fql_query = $(".fql-query").val();
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

// Get query
function saveFqlQuery() {
    showLoader();
    let query = $(".fql-query").val().replace(/^\s+|\s+$/g, "").replace(/\s+/g, " ");
    if(query[query.length - 1] === '.') {
        query = query.slice(query.lenght, -1);
    }
    var data = {
        "title": $(".fql-title").val(),
        "query": query
    }
    $.ajax({
        method: 'POST',
        url: apiUrl + "/v2/fql/save",
        contentType: 'application/json',
        data: JSON.stringify(data),
        success: function (response) {
            hideLoader();
            if (response) {
                showSuccessAlert('Success', 'FQL is saved sucessfully.');
                $("#save-fql-modal").modal('hide');
                $(".fql-title").val('');
                fetchQuery();
            } else {
                showErrorAlert('Oops', "No response found");
            }
        },
        error: function (xhr, textStatus, error) {
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
    if (obj.length > 0) {
        var list = '';
        $.each(obj, function (key, value) {
            list += "<li class='list'><label>" + value.query + "</label></li>";
        })
        if (obj.length % 20 === 0 && obj.length >= AutoCallApiStartIndex) {
            // AutoCallApiStartIndex += 20;
            list += "<li id='more'><label>more...</label></li>";
          }
            // list += "<li id='more'><label>more...</label></li>";
        
        $("#auto-suggest").append(list);
        $("#auto-suggest").show();
    } else {
        $("#auto-suggest").hide();
    }
}

function panelSavedQuery(obj) {
    $('.query-list').empty();
    if (obj.length > 0) {
        var list = '';
        saveFqlQuerys = [];
        $.each(obj, function (key, value) {
            list += "<li class='list'><label>" + value.query + "</label></li>";
            saveFqlQuerys.push(value.query);
        })
        if (obj.length % 20 === 0 && obj.length >= AutoCallApiStartIndex) {
            // AutoCallApiStartIndex += 20;
            list += "<li id='more'><label>more...</label></li>";
          }
            // list += "<li id='more'><label>more...</label></li>";

        
        $(".query-list").append(list);
        $(".query-list").show();
    } else {
        $(".query-list").hide();
    }
}

$(".query-list").on("click", ".list", function () {
    $(".fql-query").val($(this).text());
});

$('.query-list').on('click', '#more', function() {
    AutoCallApiStartIndex += 20;
    triggerAPI();
});
$("#auto-suggest").on("click", ".list", function () {
    $(".fql-query").val($(this).text())
    $("#auto-suggest").hide();
});

$(".tab-name-saved").click(function () {
    //$('#auto-suggest').show();
    $(".saved-tab-style").css({"background-color": "#32a6f9"});
    $(".tab-name-saved").css({"color": "white"});
    $(".history-tab-style").css({"background-color": "white"});
    $(".tab-name-history").css({"color": "#222222"});

});

$(".history-tab-style").click(function () {
    //$('#auto-suggest').show();
    $(".history-tab-style").css({"background-color": "#32a6f9"});
    $(".tab-name-history").css({"color": "white"});
    $(".saved-tab-style").css({"background-color": "white"});
    $(".tab-name-saved").css({"color": "#222222"});
});

function triggerAPI() {
    
        var data = {
            "title": "",
            from: 0,
            size: AutoCallApiStartIndex,
        };
    
        $.ajax({
        method: 'POST',
        url: apiUrl + "/v2/fql/get",
        contentType: 'application/json',
        data: JSON.stringify(data),
        success: function (response) {
            if (response) {
                savedFqlQuery = [...response]
                console.log('response', response)
                //generateAutoSugest(response);
                if(response.length > 0) {
                panelSavedQuery(response);
                $(".saved-tab-style").css({"background-color": "#32a6f9"});
                $(".tab-name-saved").css({"color": "white"});
                $(".history-tab-style").css({"background-color": "white"});
                $(".tab-name-history").css({"color": "#222222"});
                $(".no-saved-query").hide();
                } else if (response.length === 0) {
                    $(".no-saved-query").show();
                }

            } else {
                $("#auto-suggest").hide();
                //showErrorAlert('Oops', "No response found");
                $(".no-saved-query").show();
            }
        },
        error: function (xhr, textStatus, error) {
            $("#auto-suggest").hide();
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


// hide auto suggest if user clicks anywhere on screen except lis
$(document).on('click', function(e) {
  if (e.target.id === 'auto-suggest') {
      //alert('Div Clicked !!');
  } else {
      $('#auto-suggest').hide();
  }

})

$('input.queries-search').on('input',function(e){
    var dataAttribute = $('.queries-search').val();
    fetchQuery();
    if(dataAttribute !== '') {
        $('.queries-search-label').show()
        $('.queries-search').css('margin-top','0px');
        $('.query-list ').css('height', '75%');
    } else {
    $('.queries-search-label').hide()
    $('.queries-search').css('margin-top','20px');
    $('.query-list ').css('height', '77%');
    }
   });

   function fetchQuery() {
    var value = $('.queries-search').val();
    var data = {
        "title": "",
        from: 0,
        size: AutoCallApiStartIndex,
    };
    if (value.length >= 1) {
         data = {
            "title": value,
        };
    }
    
    else{
         data = {
            "title": "",
            from: 0,
            size: AutoCallApiStartIndex,
        };
    }

    $.ajax({
        method: 'POST',
        url: apiUrl + "/v2/fql/get",
        contentType: 'application/json',
        data: JSON.stringify(data),
        success: function (response) {
            if (response) {
                //generateAutoSugest(response);
                panelSavedQuery(response);
                $(".saved-tab-style").css({"background-color": "#32a6f9"});
                $(".tab-name-saved").css({"color": "white"});
                $(".history-tab-style").css({"background-color": "white"});
                $(".tab-name-history").css({"color": "#222222"});

            } else {
                $("#auto-suggest").hide();
                //showErrorAlert('Oops', "No response found");
            }
        },
        error: function (xhr, textStatus, error) {
            $("#auto-suggest").hide();
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
