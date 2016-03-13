import tornado.ioloop
import tornado.web
import tornado.httpserver

class BaseHandler(tornado.web.RequestHandler):
    def write_error(self, status_code, **kwargs):
        if "exc_info" in kwargs:
           exc_info = kwargs["exc_info"]
           error = exc_info[1]

           errormessage = "%s: %s" % (status_code, error)
           self.render("error.html", errormessage=errormessage)
        else:
           errormessage = "%s" % (status_code)
           self.render("error.html", errormessage=errormessage)
