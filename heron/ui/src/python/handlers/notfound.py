from heron.ui.src.python.handlers import base
import tornado.web

class NotFoundHandler(base.BaseHandler):
    def get(self, *args, **kwargs):
        errormessage = "Sorry, we could not find this page"
        self.render("error.html", errormessage=errormessage)
