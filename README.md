# Heron

Heron

For more information:

* [Twitter Heron: Stream Processing at
  Scale](http://dl.acm.org/citation.cfm?id=2742788) (academic paper)
* [Twitter Heron: Stream Processing at
  Scale](https://www.youtube.com/watch?v=pUaFOuGgmco) (YouTube video)
* [Flying Faster with Twitter
  Heron](https://blog.twitter.com/2015/flying-faster-with-twitter-heron) (blog
  post)

## Heron Documentation

At the moment, Heron's OSS documentation is built using
[GitBook](https://www.gitbook.com/). To build and run the docs locally, you'll
need to [install it](https://github.com/GitbookIO/gitbook). GitBook depends on
[NodeJS](https://nodejs.org/en/) and [npm](https://www.npmjs.com/).

Once you've installed GitBook, you should run `gitbook install` to make sure
that all the required modules are fetched.  GitBook documentation can be found
[here](https://help.gitbook.com/).

### Contributing to the Docs

The Heron docs are currently under very active development. Any help with
correcting factual errors, adding missing material, improving examples, and more
would be greatly appreciated.

### Building the Docs

To build the docs, simply run `gitbook build` in the `docs` directory. The
resulting artifacts are in `docs/_book`. The `_book` directory is ignored
by Git.

### Running the Docs Locally

To run the docs locally, navigate to the `docs` directory in the Heron repo
and run `gitbook serve`. This will start up a local web server serving the docs
on `localhost:4000`. You can specify a different port using the `--port` flag.

### Building a PDF

You can build a PDF of the docs by running `gitbook pdf` in the `docs`
directory. This will produce a PDF called `book.pdf`.

**Note**: To build PDFs locally, you will need to install
[Calibre](http://calibre-ebook.com/).

#### PDF Cover Page

If you'd like to add a custom cover page for the PDF version of the docs, create
a JPEG and save it as `cover.jpg`.

### Using GitBook Variables

GitBook enables you to set project-wide variables that can be embedded into
text. These variables can be set in `docs/book.json`, within the `variables`
object. Currently, there is one variable for the general version of the Heron
docs (`version`) and three variables corresponding to versions of the Heron API
docs: `scheduler_api_version`, `topology_api_version`, and
`metrics_api_version`. They are currently all set to `0.1.0`, which will need to
change when versioning gets settled.

You can then use those variables in text like this:

```markdown
The current version of the metrics API docs is {{book.metrics_api_version}}.
```

Please note that project-level variables don't really work in normal code
blocks. You'll have to embed special HTML to make them work. This is an
unfortunate but currently unavoidable limitation of GitBook. Examples of a
workaround can be found in `docs/contributors/custom-{scheduler,sink}.md`.

### Structure of the Docs

There are some basic components of the docs that you should be aware of:

* `index.md` is the main page of the docs.
* `SUMMARY.md` is the index for the docs. If you'd like to add new pages to the
  docs or change how things are structured in the left-hand nav, make those
  changes here and GitBook will take care of the rest.
* `book.json` provides project-level configuration
* The `node_modules` directory houses fetched dependencies (and is ignored by
  Git).
* The `_book` directory houses all generated artifacts. The `docs` directory
  holds generated HTML, while the `gitbook` directory holds all static assets.
* The remaining folders hold Markdown documentation content (`concepts`,
  `contributors`, `developers`, and `operators`).
>>>>>>> master
