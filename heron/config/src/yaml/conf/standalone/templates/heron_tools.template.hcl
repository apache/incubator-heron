job "heron-tools" {
  datacenters = ["dc1"]
  type = "service"

  constraint {
    attribute = "${attr.unique.hostname}"
    value     = <heron_tools_hostname>
  }

  group "heron-tools" {
    count = 1
    task "heron-tracker" {
      driver = "raw_exec"
      config {
        command = <heron_tracker_executable>
        args = [
        "--type=zookeeper",
        "--hostport=<zookeeper_host:zookeeper_port>",
        "--name=standalone",
        "--rootpath=heron"
       ]
      }
      resources {
        cpu    = 500 # 500 MHz
        memory = 256 # 256MB
      }
    }

    task "heron-ui" {
          driver = "raw_exec"
          config {
            command = <heron_ui_executable>
          }
          resources {
            cpu    = 500 # 500 MHz
            memory = 256 # 256MB
          }
        }
  }
}