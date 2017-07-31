function getHostUrl() {
  //return "http://foxtrot.traefik.prod.phonepe.com/foxtrot";
  return location.protocol+'//'+location.hostname+(location.port ? ':'+location.port: '')+"/foxtrot";
}
