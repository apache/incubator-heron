from heron.ui.src.python.handlers import base

class MainHandler(base.BaseHandler):
    def get(self):
        self.redirect(u"/topologies")

