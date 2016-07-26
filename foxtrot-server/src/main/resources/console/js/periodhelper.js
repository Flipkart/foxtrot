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
    if (selectedPeriodString === "custom") {
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
        return periodValue + periodUnit;
    }
    return selectedPeriodString;
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
