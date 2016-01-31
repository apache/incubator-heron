################################################################################
# Global variable to store config opts
################################################################################
config_opts = dict()

################################################################################
# Get config opts from the global variable
################################################################################
def get_heron_config():
  opt_list = []
  for (k, v) in config_opts.items():
    opt_list.append('%s=%s' % (k, v))

  all_opts = '-Dheron.options=' + (','.join(opt_list)).replace(' ', '%%%%')
  return all_opts

################################################################################
# Get config opts from the global variable
################################################################################
def get_config(k):
  global config_opts
  if config_opts.has_key(k):
    return config_opts[k]
  return None

################################################################################
# Store a config opt in global variable.
################################################################################
def set_config(k, v):
  global config_opts
  config_opts[k] = v

def clear_config():
  global config_opts
  config_opts = dict()
