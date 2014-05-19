function HostDetails(hostname, port) {
	this.hostname = hostname;
	this.port = port;
}

HostDetails.prototype.url = function(path) {
	//return "http://" + this.hostname + ":" + this.port + path
	return path
};