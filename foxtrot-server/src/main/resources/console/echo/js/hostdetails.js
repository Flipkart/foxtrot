function getHostUrl() {
  //return "http://localhost:17000/foxtrot";
  //return "https://foxtrot-internal.phonepe.com/foxtrot";
  return location.protocol+'//'+location.hostname+(location.port ? ':'+location.port: '')+"/foxtrot";
}
