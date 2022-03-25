function hexToRgbA(hex) {
    var c;
    if (/^#([A-Fa-f0-9]{3}){1,2}$/.test(hex)) {
        c = hex.substring(1).split('');
        if (c.length == 3) {
            c = [c[0], c[0], c[1], c[1], c[2], c[2]];
        }
        c = '0x' + c.join('');
        return [(c >> 16) & 255, (c >> 8) & 255, c & 255];
    }
    throw new Error('Bad Hex' + hex);
}
function colorScaleQuantize(colorPalette, maxWeight, value) {
    var totalColors = colorPalette.length
    var index = Math.ceil((value / maxWeight) * (totalColors - 1));
    var hexColor = colorPalette[index];
    return hexToRgbA(hexColor);
}
function colorScaleQuantile(colorPalette, dataArray, value) {

    var totalColors = colorPalette.length
    var totalValues = dataArray.length
    var bucketSize = Math.ceil(totalValues / totalColors)
    var ii = 1;
    var colorIndex = 0;
    while (ii <= totalColors) {
        var index = ii * bucketSize;
        if (index >= totalValues) {
            colorIndex = totalColors - 1;
            break;
        }
        if (dataArray[index] >= value) {
            colorIndex = (ii - 1);
            break;
        }
        ii++;
    }
    var hexColor = colorPalette[colorIndex];
    return hexToRgbA(hexColor);
}
function colorScale(colorPalette, dataArray, maxWeight, value, quantize) {
    if (quantize) {
        return colorScaleQuantize(colorPalette, maxWeight, value);
    }
    return colorScaleQuantile(colorPalette, dataArray, value);
}