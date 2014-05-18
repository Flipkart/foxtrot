function Colors (colorCount) {
	this.palette = this.generatePalette(colorCount);
	this.colorCount = 10;//TODO::GENERATE PALETTE::colorCount;
	this.nextColorId = 0;
}

Colors.paletteTemplate = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'];

Colors.prototype.nextColor = function() {
	var color = this.palette[this.nextColorId];
	this.nextColorId = (this.nextColorId + 1) % this.colorCount;
	return color;
};

Colors.prototype.generatePalette = function(colorCount) {
	if(colorCount <= Colors.paletteTemplate.length) {
		var colors = [];
		for (var i = 0; i < colorCount; i++) {
			colors.push(Colors.paletteTemplate[i]);
		}
		return colors;
	}
	//TODO::GENERATE PALETTE
};
