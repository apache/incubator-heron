job "apiserver" {
  datacenters = ["dc1"]
  type = "service"

  constraint {
    attribute = "${attr.unique.hostname}"
    value     = <heron_apiserver_hostname>
  }

  group "apiserver" {
    count = 1
    task "apiserver" {
      driver = "raw_exec"
      config {
        command = <heron_apiserver_executable>
        args = [
        "--cluster", "standalone",
        "--base-template", "standalone",
        "-D", "heron.statemgr.connection.string=<zookeeper_host:zookeeper_port>",
        "-D", "heron.nomad.scheduler.uri=<scheduler_uri>",
        "-D", "heron.class.uploader=org.apache.heron.uploader.http.HttpUploader",
        "--verbose"]
      }
      resources {
        cpu    = 500 # 500 MHz
        memory = 256 # 256MB
      }
    }
  }
}
