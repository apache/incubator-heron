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
        args = ["--cluster", "standalone"],
      }
      resources {
        cpu    = 500 # 500 MHz
        memory = 256 # 256MB
      }
    }
  }
}