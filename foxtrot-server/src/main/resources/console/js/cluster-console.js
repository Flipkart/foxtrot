$.tablesorter.addParser({ 
        // set a unique id 
        id: 'sizesorter', 
        is: function(s, table, node) { 
            // return false so this parser is not auto detected 
            return node.cellIndex != 0; 
        }, 
        format: function(s) {
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

function bytesToSize(bytes) {
   if(bytes == 0) return '0 Byte';
   var k = 1000;
   var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
   var i = Math.floor(Math.log(bytes) / Math.log(k));
   return (bytes / Math.pow(k, i)).toPrecision(3) + ' ' + sizes[i];
}

function toPercentage(num, den) {
	return Math.round((num/den)*100) || 0;
}

function HostData() {
	this.name = "";
	this.ip = "";
	this.host = "";
	this.load = 0;
	this.memoryUsed = 0;
	this.diskUsed = 0;
	this.jvmUsed = 0;
	this.jvmSize = "";
	this.jvmOldgen = 0;
	this.jvmEden = 0;
	this.fieldCache = 0;
	this.fieldCacheAbs = 0;
	this.fieldCacheEvictions = 0;
	this.filterCache = "";
	this.filterCacheEvictions = 0;
}

function Table() {
	this.name = "";
	this.days = 0;
	this.events = 0;
	this.size = "";
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

$(".search-input").keyup(function() {
	var searchText = $(".search-input").val();	
	for(var i in hosts) {
		
	}
})

EventBus.addEventListener('hosts_loaded', function(event, data){
	if(!data.hasOwnProperty('nodes')) {
		return;
	}	
	var nodes = data['nodes'];
	var hosts = [];
	for(var nodeId in nodes) {
		var node = nodes[nodeId];
		var host = new HostData();
		host.name = node.name;
		host.ip = node.transport_address;
		host.host = node.host;
		host.load = node.os.load_average[0];
		host.memoryUsed = node.os.mem.used_percent;
		host.diskUsed = toPercentage(node.fs.total.total_in_bytes - node.fs.total.free_in_bytes, node.fs.total.total_in_bytes)
		host.jvmUsed = node.jvm.mem.heap_used_percent
		host.jvmSize = bytesToSize(node.jvm.mem.heap_used_in_bytes)
		host.jvmOldgen = toPercentage(node.jvm.mem.pools.old.used_in_bytes,node.jvm.mem.pools.old.max_in_bytes);
		host.jvmEden = toPercentage(node.jvm.mem.pools.young.used_in_bytes,node.jvm.mem.pools.young.max_in_bytes);
		if(node.hasOwnProperty['fielddata_breaker']) {
    		host.fieldCache = toPercentage(node.fielddata_breaker.estimated_size_in_bytes,node.fielddata_breaker.maximum_size_in_bytes);
		}
		else {
    		host.fieldCache = "100";
		}
		host.fieldCacheAbs = bytesToSize(node.indices.fielddata.memory_size_in_bytes);
		host.fieldCacheEvictions = node.indices.fielddata.evictions
		host.filterCache = bytesToSize(node.indices.filter_cache.memory_size_in_bytes);
		host.filterCacheEvictions = node.indices.filter_cache.evictions;
		hosts.push(host);
		
	}
	$('.header').find("p").text("Cluster: " + data['cluster_name']);
	$('.data-area').html(handlebars("#hosts-template", {hosts: hosts}));
	$(".data-table").tablesorter(
		{
			sortList: [[0,0]]
		});
});

EventBus.addEventListener('indices_loaded', function(event, data){
	if(!data.hasOwnProperty('indices')) {
		return;
	}
	var indices = data['indices'];
	var indexTable = {}
	for(var indexName in indices) {
		var normalizedName = indexName.replace(/^foxtrot-/,"").replace(/-table-[0-9\-]+$/,"");
		if(!indexTable.hasOwnProperty(normalizedName)) {
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
	for(var i in indexTable) {
		var table = new Table();
		var rawTable = indexTable[i];
		table.name = rawTable.name;
		table.days = rawTable.days;
		table.events = rawTable.events;
		table.size = bytesToSize(rawTable.size);
		tables.push(table);
	}
	$('.table-data-area').html(handlebars("#tables-template", {tables: tables}));
	$(".table-data-table").tablesorter(
		{
			sortList: [[3,1]]
		});
})

EventBus.addEventListener('cluster_loaded', function(event, data) {
	$('.cluster-data-area').html(handlebars("#cluster-template", {cluster: cluster}));
});

var esConfig = null

function getESHost() {
    if(!esConfig) {
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

    if(!$("#clusterTab").find("#elasticsearch-li").hasClass("active")) {
        return;
    }
    if(!dataLoadComplete) {
        console.warn("Skipping node data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if(!hostName) {
        console.log("Did not find an ES host");
        return;
    }
    dataLoadComplete = false;
	$.ajax({
		type: 'GET',
		url: 'http://' + hostName + ':9200/_nodes/stats',
		success: function(data) {
			EventBus.dispatch('hosts_loaded', this, data);
		}
	})
	.always(function (){
	    dataLoadComplete = true;
	});
}

function loadIndexData() {
    if($('.auto-refresh:checked').length == 0) {
        return;
    }
    if(!$("#clusterTab").find("#foxtrot-li").hasClass("active")) {
        return;
    }
    if(!indexLoadComplete) {
        console.warn("Skipping index data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if(!hostName) {
        console.log("Did not find an ES host");
        return;
    }
    indexLoadComplete = false;
	$.ajax({
		type: 'GET',
		url: 'http://' + hostName + ':9200/foxtrot-*-table-*/_stats/docs,store',
		success : function(data) {
			cluster.documentCount  = data._all.primaries.docs.count;
			cluster.dataSize = bytesToSize(data._all.primaries.store.size_in_bytes);
			cluster.replicatedDataSize = bytesToSize(data._all.total.store.size_in_bytes);
			EventBus.dispatch('cluster_loaded', this, data);
			EventBus.dispatch('indices_loaded', this, data);
		}
	})
	.always(function(){
        indexLoadComplete = true;
    });
}

function loadClusterHealth() {
    if($('.auto-refresh:checked').length == 0) {
        return;
    }
    if(!$("#clusterTab").find("#elasticsearch-li").hasClass("active")) {
        return;
    }
    if(!clusterLoadComplete) {
        console.warn("Skipping cluster data load as last run is not complete...");
        return;
    }
    var hostName = getESHost();
    if(!hostName) {
        console.log("Did not find an ES host");
        return;
    }
    clusterLoadComplete = false;
	$.ajax({
		type: 'GET',
		url: 'http://' +  hostName + ':9200/_cluster/health',
		success : function(data) {
			cluster.name = data.cluster_name;
			cluster.status = data.status;
			cluster.numNodes = data.number_of_nodes;
			cluster.numDataNodes = data.number_of_data_nodes;
			cluster.activePrimaryShards = data.active_primary_shards;
			cluster.activeShards = data.active_shards;
			cluster.relocatingShards = data.relocating_shards;
			cluster.initializingShards = data.initializing_shards;
			cluster.unassignedShards = data.unassigned_shards;
			EventBus.dispatch('cluster_loaded', this, data);
		}
	})
	.always(function() {
        clusterLoadComplete = true;
    });
}

$('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
    loadClusterHealth();
    loadData();
  	loadIndexData();
});

$(document).ready(function() {
    $.ajax({
        type: 'GET',
        url: '/foxtrot/v1/util/config',
        success: function(data) {
            esConfig = data['elasticsearch'];
            $('#clusterTab a:last').tab('show');
            loadClusterHealth();
            loadData();
            loadIndexData();
            setInterval(loadClusterHealth, 10000);
            setInterval(loadData, 10000);
            setInterval(loadIndexData, 10000);
        }
    });
    //$("#clusterTab a:first").tab('show');

});