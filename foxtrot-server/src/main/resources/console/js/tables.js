function Table(name, ttl) {
	this.name = name;
	this.ttl = ttl;
}

function Tables() {
	this.tables = [];
	this.tableChangeHandlers = []
	this.selectedTable = null;
	this.currentTableFieldMappings = null;
}

Tables.prototype.init = function(callback) {
	$.ajax({
		url: hostDetails.url("/foxtrot/v1/tables"),
		contentType: "application/json",
		context: this,
		success: function(tables) {
			this.tables = [];
			for (var i = tables.length - 1; i >= 0; i--) {
				var table = tables[i];
				this.tables.push(new Table(table.name, table.ttl));
			};
			for (var i = this.tableChangeHandlers.length - 1; i >= 0; i--) {
				this.tableChangeHandlers[i](this.tables);
			};
			this.selectedTable = this.tables[0];
		}
	});	
};

Tables.prototype.registerTableChangeHandler = function(tableChangeHandler) {
	this.tableChangeHandlers.push(tableChangeHandler);
};

Tables.prototype.loadTableMeta = function(table) {
	$.ajax({
		url: hostDetails.url("/foxtrot/v1/tables/" + table + "/fields"),
		contentType: "application/json",
		context: this,
		success: $.proxy(function(data){
			this.currentTableFieldMappings = data.mappings;
		}, this)
	});	
};
