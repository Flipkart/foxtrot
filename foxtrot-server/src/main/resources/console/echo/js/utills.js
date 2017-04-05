// Return data format like yyyy-mm-dd
function formatDate(date) {
  var d = new Date(date),
      month = '' + (d.getMonth() + 1),
      day = '' + d.getDate(),
      year = d.getFullYear();

  if (month.length < 2) month = '0' + month;
  if (day.length < 2) day = '0' + day;
  //[year, month, day].join('-');
  return day;
}

function periodFromWindow(periodUnit, customPeriodString) {
    if (!customPeriodString) {
        return "days";
    }

    if (customPeriodString == "custom") {
        return periodUnit;
    }
    if (customPeriodString === "1d") {
        return "hours";
    }
    if (customPeriodString.endsWith("d")) {
        return "days";
    }
    return "days";
}

function timeValue(periodUnit, periodValue, selectedPeriodString) {
    var timestamp = new Date().getTime();
    if (selectedPeriodString === "custom" || !selectedPeriodString) {
        return {
            field: "_timestamp",
            operator: "last",
            duration: periodValue + periodUnit,
            currentTime: timestamp
        };
    }
    return {
        operator: "last",
        duration: selectedPeriodString
    };
}

function getPeriodString(periodUnit, periodValue, selectedPeriodString) {
    if (selectedPeriodString === "custom") {
        return periodValue + labelPeriodString(periodUnit);
    }
    return selectedPeriodString;
}

function labelPeriodString(periodString) {
    if (!periodString) {
        return null;
    }

    if (periodString == "days") {
        return "d";
    } else if (periodString == "hours") {
        return "h";
    } else {
        return "m";
    }
}

function axisTimeFormat(periodUnit, customPeriod) {
    var period = periodFromWindow(periodUnit, customPeriod);

    if (period == "hours" || period == "minutes") {
        return "%H:%M %e %b";
    }

    if (period == "days") {
        return "%e %b";
    }

    return "%e %b";
}

function findIndex(currentTabName) {
  var index = -1;
  for(var i = 0; i< globalData.length; i++) {
    for(var indexData in globalData[i]){
      if(indexData == currentTabName)
        index = i;break;
    }
  }
  return index;
}
