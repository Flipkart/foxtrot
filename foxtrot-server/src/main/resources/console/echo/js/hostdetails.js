function getHostUrl() {
  //return "http://localhost:17000/foxtrot";
  return "http://foxtrot.traefik.stg.phonepe.com/foxtrot"
  return location.protocol + '//' + location.hostname + (location.port ? ':' + location.port : '') + "/foxtrot";
}
