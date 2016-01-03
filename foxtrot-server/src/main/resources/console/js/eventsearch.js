function Filter() {
  this.id=null;
  this.opMeta = null;
  this.field = null;
  this.operator = null;
  this.operand1 = null;
  this.operand2 = null;
}

function BinaryOperation() {
  this.field = null;
  this.operator = null;
  this.value = null;
}

BinaryOperation.prototype.init = function(filter) {
    this.field = filter.field;
    this.operator = filter.operator;
    this.value = filter.operand1;
}

function BetweenOperation() {
  this.field = null;
  this.operator = null;
  this.from = null;
  this.to = null;
}

BetweenOperation.prototype.init = function(filter) {
    this.field = filter.field;
    this.operator = filter.operator;
    this.from = filter.operand1;
    this.to = filter.operand2;
}

var operationFactory = {
    create: function(filter, operationMeta) {
        filter.operator = operationMeta.operator;
        var operation = null;
        if(2 == operationMeta.cardinality) {
            operation = new BinaryOperation();
        }
        else {
            operation = new BinaryOperation();
        }
        operation.init(filter);
        return operation;
    }
}

function OperationMetadata(operator, cardinality, label) {
    this.operator = operator;
    this.cardinality = cardinality;
    this.label = label;
}

var opTable = new function() {
    this.opTableMap = new Object();
    this.opTableMap["equals"] = new OperationMetadata("equals", 2, "Equal to");
    this.opTableMap["not_equals"] = new OperationMetadata("not_equals", 2, "Not Equal to");
    this.opTableMap["less_than"] = new OperationMetadata("less_than", 2, "Less than");
    this.opTableMap["less_equal"] = new OperationMetadata("less_equal", 2, "Less or equal to");
    this.opTableMap["greater_than"] = new OperationMetadata("greater_than", 2, "Greater than");
    this.opTableMap["greater_equal"] = new OperationMetadata("greater_equal", 2, "Greater or equal to");
    this.opTableMap["contains"] = new OperationMetadata("contains", 2, "Contains");
    this.opTableMap["between"] = new OperationMetadata("between", 3, "Between");

    this.ops = new Object();

    this.ops["LONG"] =
    this.ops["INTEGER"] =
    this.ops["SHORT"] =
    this.ops["BYTE"] =
    this.ops["DATE"] =
    this.ops["FLOAT"] =
    this.ops["DOUBLE"] = ["equals", "not_equals", "less_than", "less_equal", "greater_than", "greater_equal", "between"];
    this.ops["BOOLEAN"] = ["equals", "not_equals"];
    this.ops["STRING"] = ["equals", "not_equals", "contains"];
}

function FilterSection(filterId, tables) {
    this.filterSet = new Object();
    this.filterId = filterId;
    this.tables = tables;
    this.currentOperator = null;
    this.from = 0;
    this.limit = 10;

    var filterSection = $(this.filterId);
    filterSection.find(".field-operator").change($.proxy(this.operatorSelectionChanged, this));
    filterSection.find(".event-fields").change($.proxy(this.fieldSelectionChanged, this));
    filterSection.find(".filter-condition-add").click($.proxy(this.addConditionClicked, this));
    filterSection.find(".date-from").datetimepicker();
    filterSection.find(".date-to").datetimepicker();
    this.tables.registerMetaLoadHandler($.proxy(this.tableChanged, this));
    filterSection.find(".run-query").click($.proxy(function() {
            this.from = 0;
            $(".filter-event-prev").removeAttr('disabled');
            $(".filter-event-next").removeAttr('disabled');
            this.runQuery();
        }, this));
    $(".filter-event-prev").click($.proxy(function() {
        if(this.from > 0) {
            this.from -= (this.limit + 1);
        }
        this.runQuery();
    }, this));
    $(".filter-event-next").click($.proxy(function() {
        this.from += (this.limit + 1);
        this.runQuery();
    }, this));
    $(".filter-event-prev").attr('disabled','disabled');
    $(".filter-event-next").attr('disabled','disabled');
}

FilterSection.prototype.runQuery = function(){
    var parent = $(".filter-event-container");
    parent.html("");
   var filters = [];
   for(var filterId in this.filterSet) {
       var filter = this.filterSet[filterId];
       filters.push(operationFactory.create(filter, filter.opMeta));
   }
   var filterSection = $(this.filterId);
   var fromDate = filterSection.find(".date-from").data("DateTimePicker").getDate().unix();
   var toDate = filterSection.find(".date-to").data("DateTimePicker").getDate().unix();
   if((fromDate - toDate) > 1000) {
       filters.push({
           field: "_timestamp",
           operator: "between",
           from: fromDate,
           to: toDate
       });
   }

   var table = this.tables.selectedTable;
   var request = {
       opcode: "query",
       table: table.name,
       filters: filters,
       sort: {
           field: "_timestamp",
           order: filterSection.find(".datasort").val()
       },
       from: this.from,
       limit: this.limit
   };
   $.ajax({
        method: 'POST',
        url: hostDetails.url("/foxtrot/v1/analytics"),
        contentType: "application/json",
        data: JSON.stringify(request),
        dataType: 'json',
        success: $.proxy(this.renderData, this)
    });
   console.log(request);
};

FilterSection.prototype.renderData = function(data) {
	if(!data.hasOwnProperty("documents") || data.documents.length == 0) {
		return;
	}
	var parent = $(".filter-event-container");
	var headers = [];
	var headerMap = new Object();
	var rows = [];
	var flatRows = [];

	for (var i = data.documents.length - 1; i >= 0; i--) {
		var flatObject = flat.flatten(data.documents[i]);
		for(field in flatObject) {
			if(flatObject.hasOwnProperty(field)) {
			    if(field === "id" || field === "timestamp") {
			        continue;
			    }
				headerMap[field]=1;
			}
		}
		flatRows.push(flatObject);
	}
	headers = Object.keys(headerMap);
	for (var i = flatRows.length - 1; i >= 0; i--) {
		var row = [];
		var flatData = flatRows[i];
		for (var j = 0; j < headers.length; j++) {
			var header = headers[j];
			if(flatData.hasOwnProperty(header)) {
				row.push(flatData[header]);
			}
			else {
			    console.log("Here for " + header);
				row.push("");
			}
		}
		rows.push(row);
	}
    for (var j = 0; j < headers.length; j++) {
        headers[j] = headers[j].replace("data.","");
    }
	var tableData = {headers : headers, data: rows};
	console.log(tableData);
	parent.html(handlebars("#eventbrowser-template", tableData));
};

FilterSection.prototype.tableChanged = function(table) {
    var fieldList = $("#event-fields");
    fieldList.find('option').remove();
    fieldList.selectpicker('refresh');
    var filterSection = $(this.filterId);
    filterSection.find(".field-operator").find("option").remove();
    filterSection.find(".filter-operator-1").closest(".form-group").removeClass("has-error");
    filterSection.find(".filter-operator-2").closest(".form-group").removeClass("has-error");
    var mappings =  this.tables.currentTableFieldMappings;
    if(mappings) {
        for(var i = 0; i < mappings.length; i++) {
            fieldList.append("<option value='" + i + "'>" + mappings[i].field + '</option>');
        }
        if(mappings.length > 0) {
            fieldList.val(0);
        }
    }
    fieldList.selectpicker('refresh');
    //filterSection.find('.date-to').data("DateTimePicker").setMaxDate(new Date().getTime());
    //filterSection.find('.date-to').data("DateTimePicker").setMinDate(new Date().getTime() - (table.ttl  * 864000));
    //filterSection.find('.date-from').data("DateTimePicker").setMaxDate(new Date().getTime());
    //filterSection.find('.date-from').data("DateTimePicker").setMinDate(new Date().getTime() - (table.ttl  * 864000));
    this.from = 0;
    $(".filter-event-container").find("table").remove();
    $(".filter-event-prev").attr('disabled','disabled');
    $(".filter-event-next").attr('disabled','disabled');
    $(this.filterId).find(".filter-row-selector-class").remove();

    if(mappings)
        fieldList.change();
}

FilterSection.prototype.fieldSelectionChanged = function() {
    var fieldIndex = $(this.filterId).find(".event-fields").val();
    var fieldMeta = this.tables.currentTableFieldMappings[fieldIndex];
    var opList = opTable.ops[fieldMeta.type];
    $(this.filterId).find(".filter-operator-1").closest(".form-group").removeClass("has-error");
    $(this.filterId).find(".filter-operator-2").closest(".form-group").removeClass("has-error");
    if(opList) {
        var select = $("#field-operator");
        select.find("option").remove();
        for(var i = 0; i < opList.length; i++) {
            select.append("<option value='" + i + "'>" + opTable.opTableMap[opList[i]].label + '</option>');
        }
        select.val(0);
        select.selectpicker('refresh');
        select.change();
    }
    else {
        console.error("No operator found for type: " + fieldMeta.type);
    }

}

FilterSection.prototype.operatorSelectionChanged = function() {
    $(this.filterId).find(".filter-operator-1").closest(".form-group").removeClass("has-error");
    $(this.filterId).find(".filter-operator-2").closest(".form-group").removeClass("has-error");
    var fieldId = $(this.filterId).find(".event-fields").val();
    var opId = $(this.filterId).find(".field-operator").val();
    var fieldType = this.tables.currentTableFieldMappings[fieldId].type;
    var opMeta = opTable.opTableMap[opTable.ops[fieldType][opId]];
    var opField1 = $(this.filterId).find(".filter-operator-1");
    var opField2 = $(this.filterId).find(".filter-operator-2");
    if(opMeta.cardinality == 2) {
        opField1.val("").prop('disabled', false);
        opField2.val("").prop('disabled', true);
    }
    else if(opMeta.cardinality == 3) {
        opField1.val("").prop('disabled', false);
        opField2.val("").prop('disabled', false);
    }
    if(-1 != ["LONG","INTEGER","SHORT","BYTE","FLOAT","DOUBLE"].indexOf(fieldType)) {
        opField1.prop('type', 'number');
        opField2.prop('type', 'number');
    }
    else if("STRING" === fieldType) {
        opField1.prop('type', 'text');
        opField2.prop('type', 'text');
    }
    else if("BOOLEAN" === fieldType) { //TODO
        opField1.prop('type', 'text');
        opField2.prop('type', 'text');
    }
    else if("DATE" === fieldType) { //TODO
        opField1.prop('type', 'datetime-local');
        opField2.prop('type', 'datetime-local');
    }
    opField1.focus();
}

FilterSection.prototype.addConditionClicked = function(){
  var filterSection = $(this.filterId);
  var fieldId = filterSection.find(".event-fields").val();
  var opId = filterSection.find(".field-operator").val();
  var fieldType = this.tables.currentTableFieldMappings[fieldId].type;
  var operator = opTable.ops[fieldType][opId];
  var opMeta = opTable.opTableMap[operator];
  var inputOperator1 = filterSection.find(".filter-operator-1");
  var inputOperator2 = filterSection.find(".filter-operator-2");
  var filter = new Filter();
  filter.field = this.tables.currentTableFieldMappings[fieldId].field;
  filter.operator = operator;
  if(opMeta.cardinality > 1) {
    if(!inputOperator1.val()) {
        inputOperator1.closest(".form-group").addClass("has-error");
        return;
    }
  }
  if(opMeta.cardinality > 2) {
    if(!inputOperator2.val()) {
        inputOperator2.closest(".form-group").addClass("has-error");
        return;
    }
  }

  if(-1 != ["LONG","INTEGER","SHORT","BYTE"].indexOf(fieldType)) {
    filter.operand1 = parseInt(inputOperator1.val());
  }
  else if(-1 != ["FLOAT","DOUBLE"].indexOf(fieldType)) {
    filter.operand1 = parseFloat(inputOperator1.val());
  }
  else if(fieldType === "BOOLEAN") {
    var value = inputOperator1.val();
    if(-1 != ["true", "TRUE", "false", "FALSE"].indexOf(value)) {
        filter.operand1 = ('true' === value.toLowerCase());
    }
    else {
        inputOperator1.closest(".form-group").addClass("has-error");
        inputOperator1.val("");
        return;
    }
  } else if(fieldType === "DATE") {
    filter.operand1 = new Date(inputOperator1.val()).getTime();
  } else {
    filter.operand1 = inputOperator1.val();
  }

  if(opMeta.cardinality > 2) {
      if(-1 != ["LONG","INTEGER","SHORT","BYTE"].indexOf(fieldType)) {
        filter.operand2 = parseInt(inputOperator2.val());
      }
      else if(-1 != ["FLOAT","DOUBLE"].indexOf(fieldType)) {
        filter.operand2 = parseFloat(inputOperator2.val());
      }
      else if(fieldType === "BOOLEAN") {
        var value = inputOperator2.val();
        if(-1 != ["true", "TRUE", "false", "FALSE"].indexOf(value)) {
            filter.operand2 = ('true' === value.toLowerCase());
        }
        else {
            inputOperator2.closest(".form-group").addClass("has-error");
            inputOperator2.val("");
            return;
        }
      } else if(fieldType === "DATE") {
        filter.operand2 = new Date(inputOperator2.val()).getTime();
      } else {
        filter.operand2 = inputOperator2.val();
      }
  }
  var id = filter.field + "-" + filter.operator + "-" + filter.operand1 + "-" + filter.operand2;

  if(this.filterSet.hasOwnProperty(id)) {
    error("Filter already exists");
    return;
  }
  filter.id = id;

  inputOperator1.closest(".form-group").removeClass("has-error");
  inputOperator2.closest(".form-group").removeClass("has-error");
  var rowName = ('filter-criteria-' + filter.id).replace(/\./g, "_");
  inputOperator1.closest("tr.filter-control").before( "<tr class='" + rowName + " filter-row-selector-class'>"
            + "<td class='control-table-cell field'>" + filter.field + "</td>"
            + "<td class='control-table-cell operator'>" + filter.operator + "</td>"
            + "<td class='col-lg-5 operand1'>" + ((filter.operand1)?filter.operand1:"") + "</td>"
            + "<td class='col-lg-5 operand2'>" + ((filter.operand2)?filter.operand2:"") + "</td>"
            + "<td style='width: 50px'>"
            + "   <button style='padding: 9px' class='filter-criteria-remove-"+ filter.id.replace(/\./g, "_") +"'>"
            + "      <span class='glyphicon glyphicon-minus' ></span>"
            + "    </button>"
            + "</td>"
            + "</tr>");
  $(".filter-main").on('click', 'button.filter-criteria-remove-' + filter.id.replace(/\./g, "_"),
                        {filterSection: this, id: filter.id},
                        function(e) {
                            $(this).closest("tr").remove();
                            e.data.filterSection.removeFilter(e.data.id);
                        });
  filter.opMeta = opMeta;
  this.filterSet[filter.id] = filter;
}

FilterSection.prototype.removeFilter = function(id) {
    delete this.filterSet[id];
    console.log("Removed data on: " + filter.id);
}


