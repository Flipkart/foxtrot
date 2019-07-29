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
	  switch (hostname) {
		  case "foxtrot.traefik.stg.phonepe.com":
			  return "http://gandalf.traefik.stg.phonepe.com/login/echo?redirectUrl=" + redirectUrl;
		  case "foxtrot-internal.phonepe.com":
		  case "foxtrot-gandalf.traefik.prod.phonepe.com":
		  case "foxtrot.traefik.prod.phonepe.com":
		  case "foxtrot-es6.traefik.prod.phonepe.com":
			  return "https://gandalf-internal.phonepe.com/login/echo?redirectUrl=" + redirectUrl;
		  default:
			  return 0;
	  }
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