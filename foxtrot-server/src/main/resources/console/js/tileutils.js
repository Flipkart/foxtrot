function drawLegend(columns, element) {
    if(!element) {
        return;
    }
    columns.sort(function (lhs, rhs){
        return rhs.data - lhs.data;
    });
    element.html(handlebars("#group-legend-template", {data: columns}));
}

function numberWithCommas(x) {
    //return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return x.toLocaleString('en-IN');
}