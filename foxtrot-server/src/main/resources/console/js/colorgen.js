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
