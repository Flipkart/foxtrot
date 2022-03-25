/**
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
function SunburstTile() {
    this.object = "";
}

function prepareNesting(array) {
    var nestingArray = [];
    $.each(array, function(index, value) {
        nestingArray.push(currentFieldList[parseInt(value)].field);
    });
    return nestingArray;
}

function getSunburstChartFormValues() {
    var timeframe = $("#sunburst-timeframe").val();
    var period = $("#sunburst-time-unit").val();
    var unique = $("#sunburst-uniqueKey").val();
    var aggregationType = $('#sunburst-aggregation-type').val();
    var aggregationField = $('#sunburst-aggregation-field').val();
    
    var nestingArray = [];
    var parentEl = $(".sunburstForm")
    for(var i = 1; i < 6; i++) { // 2,3,4,5
        var elements = $(parentEl).find("#sunburst-nesting-field"+i);
        var value = elements.val();
        if(value) {
            nestingArray.push(value)
        }
    }

   

    if(aggregationField == "null" || aggregationField == "" || aggregationField == null || aggregationField == "undefined") {
        aggregationField = "none";
      } else {
        aggregationField = currentFieldList[parseInt(aggregationField)].field
      }
      if(unique == "none" || unique == "" || unique == null) {
        unique = null;
      } else {
        unique = currentFieldList[parseInt(unique)].field
      }
    
    console.log("nestingArray.....",nestingArray)
    return {
        "nesting": prepareNesting(nestingArray),
        "timeframe": timeframe,
        "period": period,
        "uniqueCountOn": unique,
        "aggregationType":aggregationType,
        "aggregationField":aggregationField
    };
}

function setSunBurstChartFormValues(object) {
    var parentElement = $("#" + object.tileContext.chartType + "-chart-data");

    var parentEl = $(".sunburstForm");
    for(var i = 0; i < object.tileContext.nesting.length; i++) {
        var tmp = i+1;
        var elements = $(parentEl).find(".sunburst-nesting-field"+tmp);
        $(elements).val(currentFieldList.findIndex(function(person) {
            return person.field == object.tileContext.nesting[i]}))
        $("#sunburst-nesting-field"+tmp).selectpicker('refresh');
    }

    

    parentElement.find("#sunburst-timeframe").val(object.tileContext.timeframe);

    parentElement.find("#sunburst-time-unit").val(object.tileContext.period);

    parentElement.find("#sunburst-time-unit").selectpicker('refresh');

    var uniqeKey = parentElement.find("#sunburst-uniqueKey");
    uniqeKey.val(currentFieldList.findIndex(x => x.field == object.tileContext.uniqueCountOn));
    $(uniqeKey).selectpicker('refresh');

  $('#sunburst-aggregation-field').val(parseInt(currentFieldList.findIndex(x => x.field == object.tileContext.aggregationField)));
  $('#sunburst-aggregation-field').selectpicker('refresh');
  $('#sunburst-aggregation-Type').val();

}

function clearSunburstChartForm() {
    $('.sunburstForm')[0].reset();
    $(".sunburstForm").find('.selectpicker').selectpicker('refresh');
}

SunburstTile.prototype.getQuery = function(object) {
    this.object = object;
    var filters = [];
    // ------- Starts added today yesterday and daybefore yesterday---------------
 todayTomorrow(
    filters,
    globalFilters,
    getGlobalFilters,
    getPeriodSelect,
    timeValue,
    object
  );
  // ------ Ends added today yesterday and daybefore yesterday-------------------------------

    if (object.tileContext.filters) {
        for (var i = 0; i < object.tileContext.filters.length; i++) {
            filters.push(object.tileContext.filters[i]);
        }
    }

    var templateFilters = isAppendTemplateFilters(object.tileContext.table);
    if(templateFilters.length > 0) {
      filters = filters.concat(templateFilters);
    }
    
    var requestTags = {
        "widget": this.object.title,
        "consoleId": getCurrentConsoleId()
      }

    var data = {
        "opcode": "group",
        "consoleId": getCurrentConsoleId(),
        "table": object.tileContext.table,
        "filters": filters,
        "nesting": object.tileContext.nesting,
        "aggregationField":object.tileContext.aggregationField,
        "aggregationType":object.tileContext.aggregationType
        ,"sourceType":"ECHO_DASHBOARD",
        "requestTags": requestTags,
        "extrapolationFlag": false,
        "uniqueCountOn": object.tileContext.uniqueCountOn && object.tileContext.uniqueCountOn != "none" ? object.tileContext.uniqueCountOn : null
    }
    var refObject = this.object;
    $.ajax({
        method: "post",
        dataType: 'json',
        accepts: {
            json: 'application/json'
        },
        url: apiUrl + "/v2/analytics",
        contentType: "application/json",
        data: JSON.stringify(data),
        success: $.proxy(this.getData, this),
        error: function(xhr, textStatus, error) {
            showFetchError(refObject, "refresh", JSON.parse(xhr.responseText));
        }
    });
}
SunburstTile.prototype.getData = function(data) {
    this.render(data);
}

SunburstTile.prototype.render = function(data) {

    var object = this.object;
    var ctx = $("#" + object.id).find(".chart-item");

    var dataLength = Object.keys(data.result).length;

    if(dataLength == 0) {
        showFetchError(this.object, "data", null);
        $(ctx).hide();
        return;
    } else {
        hideFetchError(this.object);        
    }
    $(ctx).show();
    if($(ctx).find("#sequence").length > 0) {
        $(ctx).find("#sequence").remove();
        $(ctx).find("#explanation").remove();
        $(ctx).find("svg").remove();
    }

    var parentEl = $("#" + object.id).parent();
    $("#" + object.id).addClass('sunburst-tile');
    $(parentEl).removeClass('max-height');
    $(parentEl).addClass('sun-burst-max-height');
    var widgetHead = $("#" + object.id).find(".widget-header");
    $(widgetHead).height(60)
    $(ctx).addClass('sunburst-item')
    ctx.append('<div id="sequence"></div>');
    ctx.append('<div id="explanation" style="visibility: hidden;"><span id="percentage">1000</span><br/></div>')
    
    // Dimensions of sunburst.
    var width = 900;
    var height = 380;
    var radius = Math.min(width, height) / 2;

    // Breadcrumb dimensions: width, height, spacing, width of tip/tail.
    var b = {
        w: 185,
        h: 30,
        s: 9,
        t: 10
    };

    // make `colors` an ordinal scale
    var colors = d3.scale.category20b();

    // Total size of all segments; we set this later, after loading the data.
    var totalSize = 0;

    var vis = d3.select($(ctx)[0]).append("svg:svg")
        .attr("width", width)
        .attr("height", height)
        .append("svg:g")
        .attr("id", "container")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    var partition = d3.layout.partition()
        .size([2 * Math.PI, radius * radius])
        .value(function(d) {
            return d.size;
        });

    var arc = d3.svg.arc()
        .startAngle(function(d) {
            return d.x;
        })
        .endAngle(function(d) {
            return d.x + d.dx;
        })
        .innerRadius(function(d) {
            return Math.sqrt(d.y);
        })
        .outerRadius(function(d) {
            return Math.sqrt(d.y + d.dy);
        });

    // Use d3.csv.parseRows so that we do not need to have a header
    // row, and can receive the csv as an array of arrays.

    //var text = getText();
    //var csv = d3.csv.parseRows(text);
    //var json = buildHierarchy(csv);
    var json = getData();
    createVisualization(json);

    // Main function to draw and set up the visualization, once we have the data.
    function createVisualization(json) {

        // Basic setup of page elements.
        initializeBreadcrumbTrail();

        //d3.select("#togglelegend").on("click", toggleLegend);

        // Bounding circle underneath the sunburst, to make it easier to detect
        // when the mouse leaves the parent g.
        vis.append("svg:circle")
            .attr("r", radius)
            .style("opacity", 0);

        // For efficiency, filter nodes to keep only those large enough to see.
        var nodes = partition.nodes(json)
            .filter(function(d) {
                return (d.dx > 0.005); // 0.005 radians = 0.29 degrees
            });
        var uniqueNames = (function(a) {
            var output = [];
            a.forEach(function(d) {
                if (output.indexOf(d.name) === -1) {
                    output.push(d.name);
                }
            });
            return output;
        })(nodes);

        // set domain of colors scale based on data
        colors.domain(uniqueNames);

        // make sure this is done after setting the domain
        drawLegend();
        var path = vis.data([json]).selectAll($(ctx[0]).find("#path")[0])
            .data(nodes)
            .enter().append("svg:path")
            .attr("display", function(d) {
                return d.depth ? null : "none";
            })
            .attr("d", arc)
            .attr("fill-rule", "evenodd")
            .style("fill", function(d) {
                return colors(d.name);
            })
            .style("opacity", 1)
            .on("mouseover", mouseover);

        // Add the mouseleave handler to the bounding circle.
        d3.select($(ctx[0]).find("#container")[0]).on("mouseleave", mouseleave);
        // Get total size of the tree = value of root node from partition.
        totalSize = path.node().__data__.value;
    };
    
    var explanation = $(ctx[0]).find("#explanation");
    var percentage = $(ctx[0]).find("#explanation").find("#percentage");
    var trail = $(ctx[0]).find("#trail");
    
    // Fade all but the current sequence, and show it in the breadcrumb trail.
    function mouseover(d) {

        var percentage = (100 * d.value / totalSize).toPrecision(3);
        var percentageString = percentage + "%"+"("+numDifferentiation(d.value)+")";
        if (percentage < 0.1) {
            percentageString = "< 0.1%"+"("+numDifferentiation(d.value)+")";
        }
        d3.select($(ctx[0]).find("#explanation").find("#percentage")[0]).text(percentageString);        
        d3.select(explanation[0]).style("visibility", "");

        var sequenceArray = getAncestors(d);
        updateBreadcrumbs(sequenceArray, percentageString);

        // Fade all the segments.
        d3.selectAll($(ctx[0]).find("path"))
            .style("opacity", 0.3);

        // Then highlight only those that are an ancestor of the current segment.
        vis.selectAll("path")
            .filter(function(node) {
                return (sequenceArray.indexOf(node) >= 0);
            })
            .style("opacity", 1);
    }

    //var path = $(ctx[0])
    // Restore everything to full opacity when moving off the visualization.
    function mouseleave(d) {

        // Hide the breadcrumb trail
        d3.select($(trail)[0])
            .style("visibility", "hidden");

        // Deactivate all segments during transition.
        d3.selectAll($(ctx[0]).find("path")).on("mouseover", null);

        // Transition each segment to full opacity and then reactivate it.
        d3.selectAll($(ctx[0]).find("path"))
            .transition()
            .duration(1000)
            .style("opacity", 1)
            .each("end", function() {
                d3.select(this).on("mouseover", mouseover);
            });

        d3.select(explanation[0])
            .transition()
            .duration(1000)
            .style("visibility", "hidden");
    }

    // Given a node in a partition layout, return an array of all of its ancestor
    // nodes, highest first, but excluding the root.
    function getAncestors(node) {
        var path = [];
        var current = node;
        while (current.parent) {
            path.unshift(current);
            current = current.parent;
        }
        return path;
    }

    function initializeBreadcrumbTrail() {
        var seq = $(ctx).find("#sequence");
        // Add the svg area.
        var trail = d3.select(seq[0]).append("svg:svg")
            .attr("width", width)
            .attr("height", 50)
            .attr("id", "trail");
        // Add the label at the end, for the percentage.
        trail.append("svg:text")
            .attr("id", "endlabel")
            .style("fill", "#000");
    }

    // Generate a string that describes the points of a breadcrumb polygon.
    function breadcrumbPoints(d, i) {
        var points = [];
        points.push("0,0");
        points.push(b.w + ",0");
        points.push(b.w + b.t + "," + (b.h / 2));
        points.push(b.w + "," + b.h);
        points.push("0," + b.h);
        if (i > 0) { // Leftmost breadcrumb; don't include 6th vertex.
            points.push(b.t + "," + (b.h / 2));
        }
        return points.join(" ");
    }

    // Update the breadcrumb trail to show the current sequence and percentage.
    function updateBreadcrumbs(nodeArray, percentageString) {

        // Data join; key function combines name and depth (= position in sequence).
        //console.log(ctx.find("#trail"))
        var g = d3.select(ctx.find("#trail")[0])
            .selectAll("g")
            .data(nodeArray, function(d) {
                return d.name + d.depth;
            });

        // Add breadcrumb and label for entering nodes.
        var entering = g.enter().append("svg:g");

        entering.append("svg:polygon")
            .attr("points", breadcrumbPoints)
            .style("fill", function(d) {
                return colors(d.name);
            });

        entering.append("svg:text")
            .attr("x", (b.w + b.t) / 2)
            .attr("y", b.h / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .text(function(d) {
                return d.name;
            });

        // Set position for entering and updating nodes.
        g.attr("transform", function(d, i) {
            return "translate(" + i * (b.w + b.s) + ", 0)";
        });

        // Remove exiting nodes.
        g.exit().remove();

        // Now move and update the percentage at the end.
        d3.select(ctx.find("#trail")[0]).select("#endlabel")
            .attr("x", (nodeArray.length + 0.5) * (b.w + b.s))
            .attr("y", b.h / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .text(percentageString);

        // Make the breadcrumb trail visible, if it's hidden.
        d3.select(ctx.find("#trail")[0])
            .style("visibility", "");

    }

    function drawLegend() {

        // Dimensions of legend item: width, height, spacing, radius of rounded rect.
        var li = {
            w: 75,
            h: 30,
            s: 3,
            r: 3
        };

        var legend = d3.select("#legend").append("svg:svg")
            .attr("width", li.w)
            .attr("height", colors.domain().length * (li.h + li.s));

        var g = legend.selectAll("g")
            .data(colors.domain())
            .enter().append("svg:g")
            .attr("transform", function(d, i) {
                return "translate(0," + i * (li.h + li.s) + ")";
            });

        g.append("svg:rect")
            .attr("rx", li.r)
            .attr("ry", li.r)
            .attr("width", li.w)
            .attr("height", li.h)
            .style("fill", function(d) {
                return colors(d);
            });

        g.append("svg:text")
            .attr("x", li.w / 2)
            .attr("y", li.h / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .text(function(d) {
                return d;
            });
    }

    function toggleLegend() {
        var legend = d3.select("#legend");
        if (legend.style("visibility") == "hidden") {
            legend.style("visibility", "");
        } else {
            legend.style("visibility", "hidden");
        }
    }

    function checkIsObject(obj) {
        if(typeof obj === "object") {
            return true;
        } else {
            return false;
        }
        return false;
    }

    function getData() {
        function for_child(item,source,res) {
			var rootName = '';
			var isNotObject = false;
			var dummy = [];
			for(var child in item) {
                chld2=[];
				if(checkIsObject(item[child])) {
					chld2.push({"name": child, "children": ''});
					chld2 = for_child(item[child], "child",chld2);
					dummy.push(chld2[0]);
				} else {
					dummy.push({"name": child, "size":(!isNaN(item[child]) ? item[child] : 0)})
				}
            }
            res[0]["children"] = dummy
			return res;
		}
        function prepareDumb(item) {
            var dum = [];
            for(var child in item) {
               
                if(item.hasOwnProperty(child)) {
                    var obj = [];
                    obj.push({"name": child, "children": ''});
                    //console.log(index)
                    obj = for_child(item[child], "root",obj); // traverse each children
                    //console.log(obj[0]["children"])
                    dum.push({"name": child, "children": obj[0]["children"]});
                }
            }
            console.log({"name": "root", "children": dum})
            //console.log(JSON.stringify(dum))
            return {"name": "root", "children": dum};
        }
        return prepareDumb(data.result);        
    }
}




//  -------------------- Starts Added download widget 2 --------------------


SunburstTile.prototype.downloadWidget = function(object) {
    this.object = object;
    var filters = [];
    // ------- Starts added  download for today yesterday and daybefore yesterday---------------
 todayTomorrow(
    filters,
    globalFilters,
    getGlobalFilters,
    getPeriodSelect,
    timeValue,
    object
  );
  // ------ Ends added today yesterday and daybefore yesterday-------------------------------

    if (object.tileContext.filters) {
        for (var i = 0; i < object.tileContext.filters.length; i++) {
            filters.push(object.tileContext.filters[i]);
        }
    }

    var templateFilters = isAppendTemplateFilters(object.tileContext.table);
    if(templateFilters.length > 0) {
      filters = filters.concat(templateFilters);
    }

    var requestTags = {
        "widget": this.object.title,
        "consoleId": getCurrentConsoleId()
      }

    var data = {
        "opcode": "group",
        "consoleId": getCurrentConsoleId(),
        "table": object.tileContext.table,
        "filters": filters,
        "nesting": object.tileContext.nesting,
        "sourceType":"ECHO_DASHBOARD",
        "requestTags": requestTags,
        "extrapolationFlag": false
    }
    var refObject = this.object;
    $.ajax({
        url: apiUrl + "/v2/analytics/download",
        type: 'POST',
        data: JSON.stringify(data),
        dataType: 'text',

        contentType: 'application/json',
        context: this,
        success: function(response) {
          downloadTextAsCSV(response, 'SunburstChart.csv')
        },
        error: function(xhr, textStatus, error ) {
          console.log("error.........",error,textStatus,xhr)
        }
    });
}

//  -------------------- Ends Added download widget 2 --------------------
