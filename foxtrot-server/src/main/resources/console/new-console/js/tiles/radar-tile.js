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

function RadarTile() {

}

RadarTile.prototype.render = function (newDiv, object) {
  var min = 100;
	var max = 10000;
  var chartObject = {
    "8": Math.floor(Math.random() * (max - min + 1)) + min,
    "9": Math.floor(Math.random() * (max - min + 1)) + min,
    "lollypop": Math.floor(Math.random() * (max - min + 1)) + min,
    "ics": Math.floor(Math.random() * (max - min + 1)) + min,
    "marshmallow": Math.floor(Math.random() * (max - min + 1)) + min,
    "kitkat": Math.floor(Math.random() * (max - min + 1)) + min,
    "jellybean": Math.floor(Math.random() * (max - min + 1)) + min
  };
  var data = [];
	for (var key in chartObject) {
		if (chartObject.hasOwnProperty(key)) {
      data.push({axis:key, value: chartObject[key]})
		}
  }
  var d = [data];
  var chartDiv = newDiv.find(".chart-item");
	var ctx = chartDiv.find("#radar-"+object.id);
	ctx.width(ctx.width);
	ctx.height(230);
  var mycfg = {
    color: function(){
      c = ['red', 'yellow', 'pink', 'green', 'blue', 'olive', 'aqua', 'cadetblue', 'crimson'];
      m = c.length - 1;
      x = parseInt(Math.random()*100);
      return c[x%m]; //Get a random color
    },
    w: 300,
    h: 300,
  }
  RadarChart.draw("#radar-"+object.id, d, mycfg);
}
