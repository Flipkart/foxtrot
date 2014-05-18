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
		}, 5000);
	}
}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}