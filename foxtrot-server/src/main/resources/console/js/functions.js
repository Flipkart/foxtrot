/**
 * Set cookie name
 */
function getCookieConstant() {
	return "ECHO_G_TOKEN";
}

/**
 * Get login redirect url
 */
function getLoginRedirectUrl() {
	
	  var hostname = window.location.hostname;
	  var redirectUrl = encodeURIComponent(window.location.href);
      return 0;
	}
	
	/**
	 * Read cookie to check user is logged in or not
	 * @param {*} cname 
	 */
	function getCookie(cname) {
	  var name = cname + "=";
	  var decodedCookie = decodeURIComponent(document.cookie);
	  var ca = decodedCookie.split(';');
	  for(var i = 0; i <ca.length; i++) {
		var c = ca[i];
		while (c.charAt(0) == ' ') {
		  c = c.substring(1);
		}
		if (c.indexOf(name) == 0) {
		  return c.substring(name.length, c.length);
		}
	  }
	  return "";
	}
	
	/**
	 * Check user is logged in
	 */
	function isLoggedIn() {
    	return true; // logged in
	}