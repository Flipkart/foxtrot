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

/* Variables */
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
var apiUrl = getHostUrl();
var interval = null;
var consoleList = [];
var currentConsoleName;
var isCopyWidget = false;
var lastConsoleName = "";
var globalFilters = false;
var isNewConsole = false;
var tablesToRender = [];
var tableFiledsArray = {};
var previousWidget = "";
var isNewRowCount = 0;
var firstWidgetType = "";
var smallWidgetCount = 0;
var editingRow = 0;
var isEdit = false;
var tileColumn = 1;
var sectionNumber = 0;
var sections = [];
var tableNameList = [];

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

  // check for basic form
  if(!$("#basic-form").valid()) {
    return;
  }

  // check particular form

  if(!$("#"+currentChartType).valid()) {
    return;
  }

  var title = $("#tileTitle").val();
  var filterDetails = getFilters();
  var tableId = parseInt($("#tileTable").val());
  var table = this.tables.tables[tableId];
  var editTileId = $("#sidebar-tileId").val();
  var tileId = guid();
  var isChild = $(".child-tile").val();
  var periodInterval = $("#period-select").val();
  var widgetSize = "";
  var position = "";

  isChild = (isChild == 'true');
  if ($("#tileTitle").val().length == 0 || !$("#tileTable").valid() || getWidgetType() == false) {
    $(".top-error").show();
    return;
  }

  $(".top-error").hide();
  var widgetType = getWidgetType();
  if (!isChild && editTileId) tileId = editTileId;

  var objectRow = 0; var objectColumn = 0; var isnewRow = false;
  if(isEdit) {
    isnewRow = false;
    objectRow = editingRow;
  }else if(defaultPlusBtn) { // find new row
    if(panelRow.length == 0){
      objectRow = 1;
    } else {
      objectRow = panelRow.length + 1;
    }
    isnewRow = true;
    isNewConsole = true;
  } else { // get existing row column
    var splitValue = customBtn.id.split("-");
    var rowObject = panelRow[splitValue[1] - 1];
    clickedRow = rowObject.id
    objectRow = parseInt(splitValue[1]);
    isnewRow = false;
  }


  if(editTileId) {
    widgetSize = tileData[tileId].tileContext.widgetSize;
    position = tileData[tileId].tileContext.position;
    isnewRow = tileData[tileId].tileContext.isnewRow;
  } else {
    widgetSize = null;
    position = 1;
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
    , "position": position
    , "widgetSize" : widgetSize
  }
  context = $.extend({}, getChartFormValues(), context);
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
  showHideSideBar();
  removeFilters();
  if(isEdit) {
    clearEditFields();
  }
};

FoxTrot.prototype.addFilters = function () {
  addFilters();
}
FoxTrot.prototype.resetModal = function () {
  clearModal();
}
function clickedChartType(el) {
  // hide
  $("#table-units>form>div.table-units-active").removeClass("table-units-active");
  // show
  currentChartType = $(".chart-type").val();
  reloadDropdowns();
  invokeClearChartForm();
  $("#table-units").show();
  var chartDataEle = $("#table-units").find("#" + currentChartType + "-chart-data");
  if (chartDataEle.length > 0) {
    $(chartDataEle).addClass("table-units-active");
  }
  else {
    showHideForms(currentChartType);
  }
  $(".vizualization-type").removeClass("vizualization-type-active");
  $(el).addClass("vizualization-type-active");
}

function saveConsole() { // Save console api
  if(globalData.length > 0 && currentConsoleName !=  undefined) {
    var name =  currentConsoleName;
    for(var i = 0; i < globalData.length; i++) { // remove unwanted objects used when adding widgets
      var secArray = globalData[i].tileData;
      for(var key in  secArray) {
        var deleteObject = secArray[key];
        delete deleteObject.tileContext.tableFields;
        delete deleteObject.tileContext.editTileId;
        delete deleteObject.tileContext.tableDropdownIndex;
      }
    }
    var convertedName = convertName(name);
    var representation = {
      id: convertedName
      , name: name
      , sections: globalData
    };
    $.ajax({
      url: apiUrl+("/v2/consoles"),
      type: 'POST',
      contentType: 'application/json',
      data: JSON.stringify(representation),
      success: function(resp) {
        if(isCopyWidget) { // copy widget action
          showSuccessAlert('Success', 'Console copied Sucessfully');
          setTimeout(function(){ window.location.href = window.location.origin+window.location.pathname+"?console="+convertedName; }, 3000);
        } else {
          showSuccessAlert('Success', 'console saved sucessfully');
        }
        hideSaveConsole();
      },
      error: function() {
        var msg = isCopyWidget ? "Could not copy console" : "Could not save console";
        showErrorAlert("Oops",msg);
        hideSaveConsole();
        if(lastConsoleName.length > 0) {
          currentConsoleName = lastConsoleName;
        }
      }
    })
  } else {
    showErrorAlert("Oops",'Add atleast one widget');
    hideSaveConsole();
  }
}

function loadConsole() { // load console list api
  $.ajax({
    url: apiUrl+("/v2/consoles/"),
    type: 'GET',
    contentType: 'application/json',
    success: function(res) {
      consoleList = res;
      appendConsoleList(res);
    },
    error: function() {
      showErrorAlert("Could not save console");
    }
  })
}

function generateTabBtnForConsole(array) { // new btn for tabs
  $(".tab").empty();
  for(var i = 0; i < array.sections.length; i++) {
    generateSectionbtn(array.sections[i].name, false);
  }
  //$('.tab button:first').addClass('active');
}

function setListConsole(value) { // making current console name selected
  $("#listConsole").val(value);
  $("#save-dashboard-name").val(currentConsoleName);
}

function removeTab(btnName) { // remove tab
  btnName = btnName.split(" ").join('_');
  var removeElement = $(".tab").find("."+btnName);
  var clasName = $(removeElement).attr('class');
  if($(removeElement).hasClass('active')) {
    clearContainer();
  }
  $(".tab").find("."+btnName).remove();
}

function deletePageList(id) { // delete page
  var deleteIndex = sections.indexOf(id);
  sections.splice(deleteIndex, 1);
  var removeTabName = $("#page-lists-content").find("#page-name-"+id).val();
  var deleteIndex = -1;
  panelRow = [];
  for(var i = 0; i < globalData.length; i++) {
    if(globalData[i].name == removeTabName) {
      deleteIndex = i;
      break;
    }
  }
  $("#page-lists-content").find(".page-row-"+id).remove();
  if(deleteIndex >= 0) {
    globalData.splice(deleteIndex, 1);
  }
  removeTab(removeTabName);
}

function generateNewPageList(i, name) { // create new tab
  var pageNumber = i + 1;
  $("#page-lists-content").append('<div class="form-group page-row-'+i+'"><label class="control-label">Page: '+ pageNumber +'</label><input type="text" id="page-name-'+i+'" value="'+ (name.length > 0 ? name : '""') +'" class="form-control"><img src="img/remove.png" id="page-row-'+i+'" class="page-remove-img" onClick="deletePageList('+i+')" /></div>');
  sectionNumber = i;
  sections.push(i);
}

function generatePageList(resp) {
  for(var i = 0; i < resp.sections.length; i++) {
    generateNewPageList(i,resp.sections[i].name);
  }
}

function getConsoleById(selectedConsole) { // get particular console list
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

      // check any tab name present in url
      var tabName = getParameterByName("tab");
      var tabIndex = 0;
      if(tabName) {
        var tabIndex = res.sections.findIndex(x => x.id == tabName);
        renderTilesObject(res.sections[tabIndex].id);
        tabIndex = tabIndex+1;
      } else {
        renderTilesObject(res.sections[0].id);
        tabIndex = 1;
      }

      // make tab button active
      $('.tab button:nth-child('+tabIndex+')').addClass('active');

      getTables();
      generatePageList(res);
      setTimeout(function() { setListConsole(selectedConsole); }, 2000);
    },
    error: function() {
      showErrorAlert("Could not save console");
      setListConsole(selectedConsole);
    }
  })
}

function loadParticularConsoleList() { // reload page based on selected console
  $("#save-dashboard-name").val(currentConsoleName);
  loadParticularConsole();
}

function renderTilesObject(currentTabName) { // render tiles based on current tab
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

function clearContainer() { // clear page when switching tabs
  $(".tile-container").empty();
  $(".tile-container").append('<div class="float-clear"></div>');
  isNewRowCount = 0;
  firstWidgetType = "";
  previousWidget = "";
  panelRow = [];
  tileColumn = 1;
}

function consoleTabs(evt, el) { // logic for tab switching
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

  // adding tab name to url query parameter
  var url = getParameterByName("tab");
  var appendQuery = currentTab.trim().toLowerCase().split(' ').join("_");
  if(url) {
    var fullUrl = window.location.href;
    var newUrl = fullUrl.substr(0, fullUrl.indexOf('&'));
    window.history.pushState(null, "Echo", newUrl+"&tab="+appendQuery);
  } else {
    window.history.pushState(null, "Echo", window.location.href+"&tab="+appendQuery);
  }
  // query parameter ends

  var currentTabName = currentTab.toLowerCase();
  isNewConsole = false;
  renderTilesObject(currentTabName);
  isNewRowCount = 0;
  smallWidgetCount = 0;
  firstWidgetType = "";
}
function getTables() { // get table list
  $.ajax({
    url: apiUrl+"/v1/tables/",
    contentType: "application/json",
    context: this,
    success: function(tables) {
      for (var i = tables.length - 1; i >= 0; i--) {
        tableNameList.push(tables[i].name)
      }
    }});
}

function generateSectionbtn(tabName, isNew) {
  var className = tabName.split(" ").join('_');
  $(".tab").append('<button class="tablinks '+className+'" id="'+tabName+'" onclick="consoleTabs(event, this)">'+tabName+'</button>');
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

function clearForms() { // clear all details
  clearModal();
  clearContainer();
  globalData = [];
  tileData = {};
  tileList = [];
  panelRow = [];
}

function showDashboardBtn() { // dashboard modal
  $("#saveConsole").show();
  $("#default-btn").show();
  $(".global-filters, .refreshtime-block, #top-settings").show();
  $("#add-page-btn").show();
}

function clearPageSettings() { // page modal
  sections = [];
  $("#page-lists-content").empty();
}

function createDashboard() { // create dashboard
  var tabName = $("#tab-name").val();
  var dashboardName = $(".dashboard-name").val();
  currentConsoleName = dashboardName;
  $(".tab").empty();
  generateSectionbtn(tabName, true);
  $("#addDashboard").modal('hide');
  $(".dashboard-name").val('');
  $(".save-dashboard-name").val(currentConsoleName);
  $("#tab-name").val('');
  $("#listConsole").val('none');
  clearForms();
  showDashboardBtn();
  clearPageSettings();
  generateNewPageList(0, tabName);
}

function addSections() { // page sections
  var tabName = $("#section-name").val();
  generateSectionbtn(tabName, true);
  $("#section-name").val('');
}
function clearFilterValues() { // clear filter in sidebar
  $("#filter-checkbox-div").empty();
}

function clearEditFields() {
  isEdit = false;
  editingRow = 0;
}

function showHideSideBar() { // show sidebar for adding widgets
  sideBarScrollTop();
  if( $('#sidebar').is(':visible') ) {
    $('#sidebar').hide();
    $('#sidebar').find(".chart-type").attr('disabled', false);
    $(".global-filters").css({'flot' :'right'});
    $(".top-error").hide();
    $('.tile-container').find(".highlight-tile").removeClass('highlight-tile');

    setTimeout(function(){
      removeFilters();
    }, 1000);
    clearEditFields();
  }
  else {
    $('#sidebar').show();
    $('#sidebar').css({ 'width': '356px' });
    $(".delete-widget").hide();
    $("#delete-widget-divider").hide();
  }
}

function savePageSettings() { // save page settings modal
  for(var i = 0; i<sections.length; i++) {
    var nthNumber = i + 1;
    var ele = $( ".tab button:nth-child("+nthNumber+")" );
    var newName = $("#page-lists-content").find("#page-name-"+sections[i]).val();
    $( ele ).text(newName);
    $(ele).attr('id', newName);
    var id = convertName(newName);
    if(i >= globalData.length) {
      generateSectionbtn(newName, true);
    } else {
      globalData[i].name = newName;
      globalData[i].id = id;
    }
  }
  currentConsoleName = $("#page-dashboard-name").val();
  $(".save-dashboard-name").val(currentConsoleName);
  showHidePageSettings();
}

function showHidePageSettings() { // page setting modal
  $(".page-dashboard-name").val(currentConsoleName);
  if( $('#page-settings').is(':visible') ) {
    $('#page-settings').hide();
  }
  else {
    $('#page-settings').show();
    $('#page-settings').css({ 'width': '356px' });
  }
}

$(document).ready(function () {
  var type = $("#widgetType").val();
  var foxtrot = new FoxTrot();
  $("#addWidgetConfirm").click($.proxy(foxtrot.addTile, foxtrot));
  $("#sidebar-filter-btn").click($.proxy(foxtrot.addFilters, foxtrot));
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
  $(".copy-dashboard-submit").click(function (e) {
    if($(".copy-dashboard-name").val().length == 0) {
      $(".copy-db-error").show();
      return;
    } else {
      $(".copy-db-error").hide();
    }
    lastConsoleName = currentConsoleName;
    isCopyWidget = true;
    currentConsoleName = $("#copy-dashboard-name").val();
    saveConsole();
  });
  $("#listConsole").change(function () {
    loadParticularConsoleList();
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
      resetPeriodDropdown();
    }
  });

  var consoleId = getParameterByName("console").replace('/','');
  if(consoleId) {
    getConsoleById(consoleId);
    isNewConsole = false;
  } else {
    isNewConsole = true;
  }

  var isOpenDashboard = getParameterByName("openDashboard");
  if(isOpenDashboard) {
    $("#addDashboard").modal('show');
  }

  $(".delete-widget-btn").click( function() {
    var id = $("#delete-widget-value").val();
    $(".tile-container").find('#'+id).remove();
    deleteWidget(id);
  });

  // Scroll to new copied div
  function goToWidget(id) {
    document.getElementById(id).scrollIntoView({
      behavior: 'smooth'
    });
  }

  // function to insert as a new row of copied widget
  function insertNewRow(object) {
    console.log(object);
    var lastItem = tileList[tileList.length-1];
    var newRow = tileData[lastItem].tileContext.row + 1;

    // create a new object
    var newRowObject = JSON.parse(JSON.stringify(object));
    newRowObject.tileContext.row = newRow;
    newRowObject.tileContext.position = 1;
    newRowObject.id = guid();
    newRowObject.title = newRowObject.title+" - copy";
    newRowObject.tileContext.isnewRow = true;
    
    // add new object to tilelist and tiledata
    tileList.push(newRowObject.id);
    tileData[newRowObject.id] = newRowObject;

    // create copied tiles
    renderTiles(newRowObject);
    showHideSideBar(); // close sidebar
    goToWidget(newRowObject.id);
  }

  // function to insert a new row of copied widget into existing row
  /**
    * isLastRow true means the copied object can be fit into the last row
    * Else it will rendered as an new row
  */
  function insertIntoExistingRow(object, isLastRow, lastRowValue) {
    var row  = 0;

    if(!isLastRow) {
      row = object.tileContext.row
    } else {
      row = lastRowValue;
    }

    var indexOfClickedObject = tileList.indexOf(object.id);

    // create a new object
    var newRowObject = JSON.parse(JSON.stringify(object));
    newRowObject.tileContext.row = row;
    newRowObject.tileContext.isnewRow = false;

    newRowObject.id = guid();
    newRowObject.title = newRowObject.title+" - copy";
    
    // add new object to tilelist and tiledata

    if(isLastRow) {
      tileList.push(newRowObject.id); // add at end
    } else {
      tileList.splice(indexOfClickedObject+1, 0, newRowObject.id); // add at index
    }

    tileData[newRowObject.id] = newRowObject;

    // create copied tiles
    renderTiles(newRowObject);
    showHideSideBar(); // close sidebar
    goToWidget(newRowObject.id);
  }

  /**
   * Check space is available in last row
   * if available insert new tile in last row
   * Else insert as an new row
   */
  function findSpaceAvailableInLAstRow(clickedObject) {
    var getLastElement = tileList[tileList.length - 1];
    var findLastRowSpace = findSpaceAvailable(tileData[getLastElement].tileContext.row, function(val) {
      if(val > 0 & val < 12) {
        insertIntoExistingRow(clickedObject, true, tileData[getLastElement].tileContext.row);
      } else {
        insertNewRow(clickedObject);
      }
    });
  }

  /**
   * trigger correct function to insert copied row
   * @param {*} totalUsedSize 
   * @param {*} clickedObject 
   */
  function triggerRenderTile(totalUsedSize , clickedObject){
    if(totalUsedSize >= 12) { 
      findSpaceAvailableInLAstRow(clickedObject);
      return;
    } else if(totalUsedSize == 9) {
      insertIntoExistingRow(clickedObject, false, 0)
      return;
    } else if(totalUsedSize == 6) {
      insertIntoExistingRow(clickedObject , false, 0);
      return;
    } else if(totalUsedSize == 3) {
      insertIntoExistingRow(clickedObject, false, 0);
      return;
    } else {
      insertNewRow(clickedObject);
      return;
    }
  }

  // find how many space left in given row
   function findSpaceAvailable(copiedRow, callback) {
    var totalUsedSize = 0; // calculate total size used in a row
    for(var loop = 0; loop < tileList.length; loop++) { // loop to find out total used size  
      if(tileData[tileList[loop]].tileContext.row == copiedRow) { // if copied row and loop row is same
        lastPosition = tileData[tileList[loop]].tileContext.position;
        totalUsedSize = totalUsedSize+getWidgetSize(tileData[tileList[loop]].tileContext.chartType); // calculate size
      }
    }
    callback(totalUsedSize);
    return;
    }

  $(".copy-widget-btn").click( function() {
    var clickedObject = $("#copy-widget-value").data("tile"); // Read data attributes
    var copiedRow = clickedObject.tileContext.row; // get row
    // check available space and render tile
    findSpaceAvailable(copiedRow, function(val) {
      triggerRenderTile(val, clickedObject);
    });
  })

  $("#add-new-page-list").click(function() {
    generateNewPageList(sectionNumber+1 , "");
  });

  $(".page-setting-save-btn").click(function() {
    savePageSettings();
  });

  $("#sidebar-btn-form").validate({
    submitHandler: function (form) { // for demo
      alert('valid form submitted'); // for demo
      return false; // for demo
    }
  });

  $('#table-units .selectpicker').on('change', function(){ // select picker refresh for sidebar
    var selected = $(this).val();
    if(selected) {
      $(this).next().css( "display", "none" );
    } else {
      $(this).next().css( "display", "block" );
    }
  });
  //Initialize libs
  $('.selectpicker').selectpicker();
  $('#refresh-time').tooltip(); 
});
