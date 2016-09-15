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
