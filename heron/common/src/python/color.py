import logging
import colorlog

# formatter = colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s')
formatter = colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(reset)s %(message)s')
stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
stream.setFormatter(formatter)

Log = logging.getLogger()
Log.setLevel(logging.DEBUG)
Log.addHandler(stream)
