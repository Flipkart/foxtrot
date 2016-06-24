function periodFromWindow(periodString) {
    if(periodString == "custom") {
        return "minutes";
    }
    if(periodString === "1d") {
        return "hours";
    }
    if(periodString.endsWith("d")) {
        return "days";
    }
    return "minutes";
}

function timeValue(period, selectedPeriodString, noOfDaysOld) {
    if (noOfDaysOld === undefined) noOfDaysOld = 0;

    var timestamp = new Date(new Date() - noOfDaysOld * 24 * 3600 * 1000).getTime();
    console.log("TimeStamp = " + timestamp);

    if(selectedPeriodString === "custom") {
        return {
            field: "_timestamp",
            operator: "last",
            duration: period + "m",
            currentTime: timestamp
        };
    }
    return {
        operator: "last",
        duration: selectedPeriodString,
        currentTime: timestamp
    };
}

function getPeriodString(period, selectedPeriodString) {
    if(selectedPeriodString === "custom") {
        return (period >= 60) ? ((period / 60) + "h"): (period + "m");
    }
    return selectedPeriodString;
}