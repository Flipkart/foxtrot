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

 function S4() {
    return (((1+Math.random())*0x10000)|0).toString(16).substring(1); 
}
 
function guid() {
	return (S4() + S4() + "-" + S4() + "-4" + S4().substr(0,3) + "-" + S4() + "-" + S4() + S4() + S4()).toLowerCase();
}

function error(message) {
	showAlert(message, "alert-danger", false);
}

function warn(message) {
	showAlert(message, "alert-warning", true);
}

function info(message) {
	showAlert(message, "alert-info", true);
}
function success(message) {
	showAlert(message, "alert-success", true);
}

function showAlert(message, className, autoDismiss) {
	var alert = $(".alert");
	alert.addClass(className);
	alert.find(".alert-message").html(message);
	$("[data-hide]").off("click");
	$("[data-hide]").on("click", function(){
        $(this).closest("." + $(this).attr("data-hide")).hide();
        $(".alert").removeClass(className);
    });
	alert.show();
	if(autoDismiss) {
		setTimeout(function(){
			$("[data-hide]").click();
		}, 2000);
	}
}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

String.prototype.visualLength = function() {
    var ruler = $("#ruler");
    ruler.html(this);
    return ruler.width();
}