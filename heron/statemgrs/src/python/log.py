import logging
import logging.handlers

# Create the logger
Log = logging.getLogger('heron-state')

def configure(level, logfile = None):
  log_format = "%(asctime)s-%(levelname)s: %(message)s"
  date_format = '%a, %d %b %Y %H:%M:%S'

  logging.basicConfig(format=log_format, datefmt=date_format)
  Log.setLevel(level)

  if (logfile != None):
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logging.Formatter(log_format))
    Log.addHandler(fh)
