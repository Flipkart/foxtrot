var apiUrl = getHostUrl();

$.tablesorter.addParser({
    // set a unique id
    id: 'sizesorter',
    is: function (s, table, node) {
        // return false so this parser is not auto detected
        return node.cellIndex != 0;
    },
    format: function (s) {
        s = s.trim()
        var sizeValue = s.substring(0, s.indexOf(" "))
        var sizeUnit = s.substring(sizeValue.length + 1, s.length).replace(" ", "")
        var multiplier = 1;
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
        return parseInt(sizeValue) * (1 + Math.pow(1024, sizes.indexOf(sizeUnit)));
    },
    // set type, either numeric or text
    type: 'numeric'
});

var clusterLoader = 0;
var indicesLoader = 0;

function bytesToSize(bytes) {
    if (bytes == 0) return '0 Byte';
    var k = 1000;
    var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return (bytes / Math.pow(k, i)).toPrecision(3) + ' ' + sizes[i];
}

function toPercentage(num, den) {
    return Math.round((num / den) * 100) || 0;
}

function HostData() {
    this.name = "";
    this.ip = "";
    this.host = "";
    this.load = 0;

    this.memoryTotal = 0;
    this.memoryUsed = 0;
    this.memoryUsedPercent = 0;

    this.diskTotal = 0;
    this.diskUsed = 0;
    this.diskUsedPercent = 0;

    this.jvmTotal = "";
    this.jvmUsed = 0;
    this.jvmUsedPercent = 0;

    this.fieldCache = 0;
    this.fieldCacheAbs = 0;
    this.fieldCacheEvictions = 0;
}

function Table() {
    this.name = "";
    this.days = 0;
    this.events = 0;
    this.size = "";
    this.columnCount = "";
    this.avgSize = "";
}

var cluster = {
    name: "",
    status: "",
    numNodes: 0,
    numDataNodes: 0,
    activePrimaryShards: 0,
    activeShards: 0,
    relocatingShards: 0,
    initializingShards: 0,
    unassignedShards: 0,
    documentCount: 0,
    dataSize: "",
    replicatedDataSize: ""
};

$(".search-input").keyup(function () {
    var searchText = $(".search-input").val();
    for (var i in hosts) {

    }
})

EventBus.addEventListener('hosts_loaded', function (event, data) {
    if (!data.hasOwnProperty('nodes')) {
        return;
    }
    var nodes = data['nodes'];
    var hosts = [];
    for (var nodeId in nodes) {
        var node = nodes[nodeId];
        var host = new HostData();
        host.name = node.name;
        host.ip = node.ip;
        host.host = node.host;

        host.memoryTotal = bytesToSize(node.os.mem.total_in_bytes);
        host.memoryUsed = bytesToSize(node.os.mem.used_in_bytes);
        host.memoryUsedPercent = toPercentage(node.os.mem.used_in_bytes, node.os.mem.total_in_bytes);

        host.diskTotal = bytesToSize(node.fs.total.total_in_bytes);
        host.diskUsed = bytesToSize(node.fs.total.total_in_bytes - node.fs.total.free_in_bytes);
        host.diskUsedPercent = toPercentage(node.fs.total.total_in_bytes - node.fs.total.free_in_bytes, node.fs.total.total_in_bytes);

        host.jvmTotal = bytesToSize(node.jvm.mem.heap_committed_in_bytes);
        host.jvmUsed = bytesToSize(node.jvm.mem.heap_used_in_bytes);
        host.jvmUsedPercent = toPercentage(node.jvm.mem.heap_used_in_bytes, node.jvm.mem.heap_committed_in_bytes);

        if (node.hasOwnProperty('breakers')) {
            host.fieldCache = toPercentage(
                                    node.breakers.fielddata.estimated_size_in_bytes,
                                    node.breakers.fielddata.limit_size_in_bytes);
        } else {
            host.fieldCache = "100";
        }

        if (node.indices.fielddata.hasOwnProperty("memory_size_in_bytes")) {
            host.fieldCacheAbs = bytesToSize(node.indices.fielddata.memory_size_in_bytes);
        } else {
            host.fieldCacheAbs = 'N/A'
        }

        host.fieldCacheEvictions = node.indices.fielddata.evictions;
        hosts.push(host);

    }
    $('.header').find("p").text("Cluster: " + data['clusterName']);
    $('.data-area').html(handlebars("#hosts-template", {
        hosts: hosts
    }));
    $(".data-table").tablesorter({
        sortList: [
            [0, 0]
        ]
    });
});

function formatValues(bytes, convertTo) {
    if(convertTo == "GB") {
        return(bytes / 1073741824).toFixed(2) + " GB";
    } else {
        return(bytes / 1024).toFixed(2) + " KB"
    }

}

EventBus.addEventListener('indices_loaded', function (event, data) {
    if (!data.hasOwnProperty('indices')) {
        return; 
    }
     var indices = data['indices'];
     
    var indexTable = {}
    var tableNamePrefix =  (esConfig.hasOwnProperty("tableNamePrefix"))
            ? tableNamePrefix = esConfig.tableNamePrefix
            : tableNamePrefix = "foxtrot";
    for (var indexName in indices) {
        if(!indexName.startsWith(tableNamePrefix)) {
            continue;
        }
        var normalizedName = normalizedName = indexName.replace(new RegExp("^" + tableNamePrefix + "-"), "").replace(/-table-[0-9\-]+$/, "");
        if (!indexTable.hasOwnProperty(normalizedName)) {
            indexTable[normalizedName] = {
                name: normalizedName,
                days: 0,
                events: 0,
                size: 0
            }
        }
        var indexData = indexTable[normalizedName];
        indexData.days += 1;
        indexData.events += indices[indexName].primaries.docs.count;
        indexData.size += indices[indexName].primaries.store.size_in_bytes;
    }
    var tables = []
    var tablesColumncount = loadTablesCoulmnCount();
    for (var i in indexTable) {
        var table = new Table();
        var rawTable = indexTable[i];
        table.name = rawTable.name;
        table.days = rawTable.days;
        table.events = rawTable.events;
        table.size = formatValues(rawTable.size, 'GB');
        if (data.tableColumnCount == undefined){
            table.columnCount = tablesColumncount[rawTable.name];
        }
        else{
            // table.columnCount = data.tableColumnCount[rawTable.name];
            if (data.tableColumnCount == undefined){
                table.columnCount = data[rawTable.name];
            }
            else{
                table.columnCount = data.tableColumnCount[rawTable.name];
            }

        }
        var calculateSize = rawTable.size/rawTable.events;
        table.avgSize = formatValues(calculateSize, 'KB');;
        tables.push(table);
    }
    $('.table-data-area').html(handlebars("#tables-template", {
        tables: tables
    }));
    $(".table-data-table").tablesorter({
        sortList: [
            [3, 1]
        ],
        headers  : {
            3 : { sorter : 'digit' },
            4 : { sorter : 'digit' }
          }
    });
})

EventBus.addEventListener('cluster_loaded', function (event, data) {
    $('.cluster-data-area').html(handlebars("#cluster-template", {
        cluster: cluster
    }));
});

var esConfig = null

function getESHost() {
    if (!esConfig) {
        return null;
    }
    return esConfig.hosts[Math.floor(Math.abs(Math.random() * (esConfig.hosts.length)))];
}
var clusterLoadComplete = true;
var dataLoadComplete = true;
var indexLoadComplete = true;

function loadData() {
    if($('.auto-refresh:checked').length == 0) {
        return;
    }

    if(!$(".new-header").find("#cluster-menu").find(".load-elastic").hasClass("cluster-menu-active")) {
        return;
    }

    if (!dataLoadComplete) {
        console.warn("Skipping node data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if (!hostName) {
        console.log("Did not find an ES host");
        return;
    }

    if(clusterLoader == 0) {
        showLoader();
        clusterLoader++;
    }

    dataLoadComplete = false;
    $.ajax({
            type: 'GET',
            url: apiUrl+'/v1/clusterhealth/nodestats',
            success: function (data) {
                hideLoader();
                EventBus.dispatch('hosts_loaded', this, data);
            }
        })
        .always(function () {
            dataLoadComplete = true;
        });
}

function loadIndexData() {
    if($('.auto-refresh:checked').length == 0) {
        return;
    }

    if(!$(".new-header").find("#cluster-menu").find(".load-foxtrot").hasClass("cluster-menu-active")) {
        return;
    }

    if (!indexLoadComplete) {
        console.warn("Skipping index data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if (!hostName) {
        console.log("Did not find an ES host");
        return;
    }

    if(indicesLoader == 0) {
        showLoader();
        indicesLoader++;
    }

    indexLoadComplete = false;
    $.ajax({

            type: 'GET',
            url: apiUrl+'/v1/clusterhealth/indicesstats',
            success: function (data) {
                hideLoader();
                if (typeof data._all.primaries.docs != "undefined") {
                    cluster.documentCount = data._all.primaries.docs.count;
                } else {
                    cluster.documentCount = 0;
                }

                if (typeof data._all.primaries.store != "undefined") {
                    cluster.dataSize = bytesToSize(data._all.primaries.store.size_in_bytes);
                } else {
                    cluster.dataSize = bytesToSize(0);
                }
                if (typeof data._all.total.store != "undefined") {
                    cluster.replicatedDataSize = bytesToSize(data._all.total.store.size_in_bytes);
                } else {
                    cluster.replicatedDataSize = bytesToSize(0);
                }
                //EventBus.dispatch('cluster_loaded', this, data);
                EventBus.dispatch('indices_loaded', this, data);
            }
        })
        .always(function () {
            hideLoader();
            indexLoadComplete = true;
        });
}

function loadTablesCoulmnCount() {
var tablesColumncount = {};
$.ajax({
    type:"GET",
    url: apiUrl + "/v1/tables/fields",
    async: false,
    contentType: "application/json",
    context: this,
    success:function(tableFieldData){
            for(var table in tableFieldData)
                {
                    var mappingSize = tableFieldData[table].mappings.length;
                    if( mappingSize == undefined)
                        mappingSize = 0;
                    tablesColumncount[table] = mappingSize;
                }
        }
    });
    return tablesColumncount;
}

function loadClusterHealth() {
    
    if($('.auto-refresh:checked').length == 0) {
        return;
    }

    if(!$(".new-header").find("#cluster-menu").find(".load-elastic").hasClass("cluster-menu-active")) {
        return;
    }

    if (!clusterLoadComplete) {
        console.warn("Skipping cluster data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if (!hostName) {
        console.log("Did not find an ES host");
        return;
    }
    clusterLoadComplete = false;
    $.ajax({
            type: 'GET',
            url: apiUrl+'/v1/clusterhealth',
            success: function (data) {
                cluster.name = data.clusterName;
                cluster.status = data.status;
                cluster.numNodes = data.numberOfNodes;
                cluster.numDataNodes = data.numberOfDataNodes;
                cluster.activePrimaryShards = data.activePrimaryShards;
                cluster.activeShards = data.activeShards;
                cluster.relocatingShards = data.relocatingShards;
                cluster.initializingShards = data.initializingShards;
                cluster.unassignedShards = data.unassignedShards;
                EventBus.dispatch('cluster_loaded', this, data);
            }
        })
        .always(function () {
            clusterLoadComplete = true;
        });
}

$(document).ready(function () {

    $(".load-foxtrot").click(function () {
        $(".load-foxtrot").addClass('cluster-menu-active');
        $(".load-elastic").removeClass('cluster-menu-active');
        $("#elasticsearch").hide();
        $("#foxtrot").show();
        loadIndexData();
    });

    $(".load-elastic").click(function () {
        $(".load-foxtrot").removeClass('cluster-menu-active');
        $(".load-elastic").addClass('cluster-menu-active');
        $("#foxtrot").hide();
        $("#elasticsearch").show();
        loadClusterHealth();
        loadData();
    });

    $.ajax({
        type: 'GET',
        url: apiUrl+'/v1/util/config',
        success: function (data) {
            esConfig = data['elasticsearch'];
            loadClusterHealth();
            loadData();
            loadIndexData();
            setInterval(loadClusterHealth, 10000);
            setInterval(loadData, 10000);
            setInterval(loadIndexData, 10000);
        }
    });
});
