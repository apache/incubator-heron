# Increase log verbosity
log_level = "DEBUG"

# Setup data dir
data_dir = "/tmp/slave"

# Enable the client
client {
    enabled = true
    servers = [<nomad_masters:master_port>]
    options = {
     "driver.raw_exec.enable" = "1"
  }
}

# Modify our port to avoid a collision with server1
ports {
    http = 5656
}
