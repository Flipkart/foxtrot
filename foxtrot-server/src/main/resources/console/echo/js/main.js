/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var tiles = {};
var tileList = [];
var tileData = {};
var panelRow = [];
var globalData = [];
var defaultPlusBtn = true;
var customBtn;
var filterRowArray = [];
var currentChartType;
var tableList = [];
var currentFieldList = [];
var apiUrl = "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
var interval = null;
var consoleList = [];
var currentConsoleName;
var globalFilters = false;
var isNewConsole = false;
var tablesToRender = [];
var tableFiledsArray = {};

function TablesView(id, tables) {
  this.id = id;
  this.tables = tables;
  this.tableSelectionChangeHandler = null;
}
TablesView.prototype.load = function (tables) {
  var select = $(this.id);
  select.find('option').remove();
  for (var i = tables.length - 1; i >= 0; i--) {
    select.append("<option value='" + i + "'>" + tables[i].name + '</option>');
  }
  select.val(this.tables.getSelectionIndex());
  select.selectpicker('refresh');
  select.change();
};
function clearModalfields() { // used when modal table changed
  reloadDropdowns(currentChartType);
  removeFilters();
}
TablesView.prototype.registerTableSelectionChangeHandler = function (handler) {
  this.tableSelectionChangeHandler = handler;
};
TablesView.prototype.init = function () {
  this.tables.registerTableChangeHandler($.proxy(this.load, this));
  $(this.id).change($.proxy(function () {
    var value = parseInt($(this.id).val());
    var table = this.tables.tables[value];
    clearModalfields();
    if(!tableFiledsArray.hasOwnProperty(table.name)) {
      if (table) {
        if (this.tableSelectionChangeHandler) {
          this.tableSelectionChangeHandler(table.name);
        }
        this.tables.loadTableMeta(table);
        this.tables.selectedTable = table;
        console.log("Table changed to: " + table.name);
        //console.log(this);
      }
    } else {
      currentFieldList = tableFiledsArray[table.name].mappings;
      reloadDropdowns();
    }
  }, this));
  this.tables.init();
};

function FoxTrot() {
  this.tables = new Tables();
  this.tablesView = new TablesView("#tileTable", this.tables);
  this.queue = new Queue();
  this.tableSelectionChangeHandler = null;
}
FoxTrot.prototype.init = function () {
  this.tablesView.registerTableSelectionChangeHandler($.proxy(function (value) {
    this.selectedTable = value;
    if (this.tableSelectionChangeHandler && value) {
      for (var i = this.tableSelectionChangeHandlers.length - 1; i >= 0; i--) {
        this.tableSelectionChangeHandlers[i](value);
      };
    }
  }, this));
  this.tablesView.init();
  this.queue.start();
};

FoxTrot.prototype.addTile = function () {
  var title = $("#tileTitle").val();
  var filterDetails = getFilters();
  var tableId = parseInt($("#tileTable").val());
  var table = this.tables.tables[tableId];
  var editTileId = $(".tileId").val();
  var tileId = guid();
  var isChild = $(".child-tile").val();
  var periodInterval = $("#period-select").val();
  isChild = (isChild == 'true');
  if ($("#tileTitle").val().length == 0 || !$("#tileTable").valid() || getWidgetType() == false) {
    $(".top-error").show();
    return;
  }
  if (getChartFormValues()[1] == false) {
    $(".top-error").show();
    return;
  }
  $(".top-error").hide();
  var widgetType = getWidgetType();
  if (!isChild && editTileId) tileId = editTileId;

  var objectRow = 0; var objectColumn = 0; var isnewRow = false;
  if(defaultPlusBtn) { // find new row
    if(panelRow.length == 0){
      objectRow = 1;
    } else {
      objectRow = panelRow.length + 1;
    }
    isnewRow = true;
  } else { // get existing row column
    var splitValue = customBtn.id.split("-");
    var rowObject = panelRow[splitValue[1] - 1];
    clickedRow = rowObject.id
    objectRow = parseInt(splitValue[1]);
    isnewRow = false;
  }

  var context = {
    "widgetType": widgetType
    , "table": table.name
    , "editTileId": editTileId
    , "tableDropdownIndex": tableId
    , "chartType": currentChartType
    , "filters": filterDetails.length == 0 ? [] : filterDetails
    , "tableFields": currentFieldList
    , "periodInterval": periodInterval
    , "uiFiltersList": []
    , "row": objectRow
    , "isnewRow": isnewRow
    , "tabName": $(".tab .active").attr('id')
  }
  context = $.extend({}, getChartFormValues()[0], context);
  var object = {
    "id": tileId
    , "title": title
    , "tileContext":context
    , "children": []
  }
  var tileFactory = new TileFactory();
  currentChartType = "";
  if (!editTileId && !isChild) { // for new tile
    tileFactory.tileObject = object;
    var foxtrot = new FoxTrot();
    addTilesList(object);
    tileFactory.create();
  }
  else { // edit tile
    tileFactory.tileObject = object;
    tileFactory.updateTileData();
  }
  //$("#addWidgetModal").modal('hide');
  showHideSideBar();
  removeFilters();
};

FoxTrot.prototype.addFilters = function () {
  addFitlers();
}


FoxTrot.prototype.resetModal = function () {
  clearModal();
}
function clickedChartType(el) {
  // hide
  $("#table-units>div.table-units-active").removeClass("table-units-active");
  // show
  currentChartType = $(".chart-type").val();
  reloadDropdowns();
  invokeClearChartForm();
  $("#table-units").show();
  var chartDataEle = $("#table-units").find("#" + currentChartType + "-chart-data");
  if (chartDataEle.length > 0) {
    //$(chartDataEle).show();
    $(chartDataEle).addClass("table-units-active");
  }
  else {
    showHideForms();
  }
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(el).addClass("vizualization-type-active");
}

function saveConsole() {
  if(globalData.length > 0 && currentConsoleName !=  undefined) {
    var name =  currentConsoleName;
    for(var i = 0; i < globalData.length; i++) {
      var secArray = globalData[i].tileData;
      for(var key in  secArray) {
        var deleteObject = secArray[key];
        delete deleteObject.tileContext.tableFields;
        delete deleteObject.tileContext.editTileId;
        delete deleteObject.tileContext.tableDropdownIndex;
      }
    }
    var representation = {
      id: name.trim().toLowerCase().split(' ').join("_")
      , name: name
      , sections: globalData
    };
    console.log(representation);
    $.ajax({
      url: apiUrl+("/v2/consoles"),
      type: 'POST',
      contentType: 'application/json',
      data: JSON.stringify(representation),
      success: function(resp) {
        alert('console saved sucessfully');
      },
      error: function() {
        error("Could not save console");
      }
    })
  }
}

function appendConsoleList() {
  var textToInsert = [];
  var i = 0;
  for (var a = 0; a < consoleList.length; a += 1) {
    textToInsert[i++] = '<option value=' + consoleList[a].id + '>';
    textToInsert[i++] = consoleList[a].name;
    textToInsert[i++] = '</option>';
  }
  $("#listConsole").append(textToInsert.join(''));
}

function loadConsole() {
  $.ajax({
    url: apiUrl+("/v2/consoles/"),
    type: 'GET',
    contentType: 'application/json',
    success: function(res) {
      consoleList = res;
      appendConsoleList();
    },
    error: function() {
      error("Could not save console");
    }
  })
}

function generateTabBtnForConsole(array) {
  $(".tab").empty();
  for(var i = 0; i < array.sections.length; i++) {
    generateSectionbtn(array.sections[i].name, false);
  }
  $('.tab button:first').addClass('active');
}

function setListConsole(value) {
  $("#listConsole").val(value);
  $("#save-dashboard-name").val(currentConsoleName);
}

function getConsoleById(selectedConsole) {
  $.ajax({
    url: apiUrl+("/v2/consoles/" +selectedConsole),
    type: 'GET',
    contentType: 'application/json',
    success: function(res) {
      currentConsoleName = res.name;
      clearContainer();
      globalData = [];
      globalData = res.sections;
      generateTabBtnForConsole(res);
      renderTilesObject(res.sections[0].id);
      getTables();
      setTimeout(function() { setListConsole(selectedConsole); }, 2000);
    },
    error: function() {
      error("Could not save console");
      setListConsole(selectedConsole);
    }
  })
}

function loadParticularConsole() {
  var selectedConsole = $("#listConsole").val();
  window.location.assign("?console=" + selectedConsole);
  $("#save-dashboard-name").val(currentConsoleName);
}

function renderTilesObject(currentTabName) {
  showDashboardBtn();
  var tabIndex = globalData.findIndex(x => x.id == currentTabName.trim().toLowerCase().split(' ').join("_"));
  if (tabIndex >= 0) {
    tileList = globalData[tabIndex].tileList;
    tileData = globalData[tabIndex].tileData;
    for (var i = 0; i < tileList.length; i++) {
      renderTiles(tileData[tileList[i]]);
    }
    fetchTableFields();
  }
}

function clearContainer() {
  $(".tile-container").empty();
  $(".tile-container").append('<div class="float-clear"></div>');
}

function consoleTabs(evt, el) {
  var currentTab = el.id;
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    if (tablinks[i].className.endsWith("active")) {
      clearInterval(interval);
      interval = 0;
      clearModal();
      tileData = {};
      tileList = [];
      panelRow = [];
    }
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  //document.getElementById(cityName).style.display = "block";
  clearContainer();
  evt.currentTarget.className += " active";
  var currentTabName = currentTab.toLowerCase();
  renderTilesObject(currentTabName);
}
var tableNameList = [];
function getTables() {
  $.ajax({
    url: "http://foxtrot.traefik.prod.phonepe.com/foxtrot/v1/tables/",
    contentType: "application/json",
    context: this,
    success: function(tables) {
      for (var i = tables.length - 1; i >= 0; i--) {
        tableNameList.push(tables[i].name)
      }
    }});
}

function generateSectionbtn(tabName, isNew) {
  $(".tab").append('<button class="tablinks" id="'+tabName+'" onclick="consoleTabs(event, this)">'+tabName+'</button>');
  $("#addTab").modal('hide');
  $("#tab-name").val('');
  var tablinks = document.getElementsByClassName("tablinks");
  if(isNew) {
    for (i = 0; i < tablinks.length; i++) {
      var element = $(tablinks[i]);
      if(tablinks[i].id == tabName) {
        element.addClass('active');
        clearContainer();
        tileList =  [];
        tileData = {};
      } else {
        element.removeClass('active');
      }
    }
  }
}

function clearForms() {
  clearModal();
  clearContainer();
  globalData = [];
  tileData = {};
  tileList = [];
  panelRow = [];
}

function showDashboardBtn() {
  $("#saveConsole").show();
  $("#default-btn").show();
  $(".global-filters").show();
  $("#add-page-btn").show();
}

function createDashboard() {
  var tabName = $("#tab-name").val();
  var dashboardName = $(".dashboard-name").val();
  currentConsoleName = dashboardName;
  $(".tab").empty();
  generateSectionbtn(tabName, true);
  $("#addDashboard").modal('hide');
  $(".dashboard-name").val('');
  $("#tab-name").val('');
  $("#listConsole").val('none');
  clearForms();
  showDashboardBtn();
}

function addSections() {
  var tabName = $("#section-name").val();
  generateSectionbtn(tabName, true);
  $("#section-name").val('');
}
function clearFilterValues() {
  $(".filter_values").empty();
}

function showHideSideBar() {
  if( $('#sidebar').is(':visible') ) {
    $('#sidebar').hide();
    $(".global-filters").css({'flot' :'right'});
    $(".top-error").hide();
    $('.tile-container').find(".highlight-tile").removeClass('highlight-tile');
  }
  else {
    $('#sidebar').show();
    $('#sidebar').css({ 'width': '356px' });
    $(".delete-widget").hide();
    $("#delete-widget-divider").hide();
  }
}

$(document).ready(function () {
  var type = $("#widgetType").val();
  var foxtrot = new FoxTrot();
  $("#addWidgetModal").validator();
  $("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
  $("#filter-add-btn").click($.proxy(foxtrot.addFilters, foxtrot));
  $("#default-btn").click(function () {
    defaultPlusBtn = true;
    foxtrot.resetModal();
    $(".settings-form").find("input[type=text], textarea").val("");
  });
  foxtrot.init();
  $("#save-dashboard-tab-btn").click(function () {
    currentConsoleName = $("#save-dashboard-name").val();
    saveConsole();
  });
  $("#listConsole").change(function () {
    loadParticularConsole();
  });
  $("#addDashboardConfirm").click(function() {
    createDashboard();
  });
  $("#addTabConfirm").click(function() {
    addSections();
  })
  loadConsole();
  $(".filter_values").multiselect({
    numberDisplayed: 0
  });
  $("#default-btn").click(function() {
    showHideSideBar();
    foxtrot.resetModal();
  });
  $(".chart-type").change(function() {
    clickedChartType(this);
  })

  $("#filter-close-btn").click(function() {
    showHideSideBar();
    foxtrot.resetModal();
  })

  $(".filter-switch").change(function () {
    if(this.checked) {
      globalFilters = true;
      showFilters();
    } else {
      globalFilters = false;
      hideFilters();
    }
  });

  var consoleId = getParameterByName("console").replace('/','');
  if(consoleId) {
    getConsoleById(consoleId);
    isNewConsole = false;
  } else {
    isNewConsole = true;
  }

  $(".delete-widget-btn").click( function() {
    var id = $("#delete-widget-value").val();
    $(".tile-container").find('#'+id).remove();
    deleteWidget(id);
  })
});
