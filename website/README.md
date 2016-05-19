# Heron Documentation

Heron's documentation was primarily built using the following components:

* [Hugo](http://gohugo.io) --- Static site generator
* [GulpJS](http://gulpjs.com) --- Build tool for static assets
* [Twitter Bootstrap](http://getbootstrap.com) --- CSS and JavaScript

## Documentation Setup

Running the Heron documentation locally requires that you have the following installed:

* [Make](https://www.gnu.org/software/make/)
* [Node.js](https://nodejs.org/en/)
* [npm](https://www.npmjs.com/)
* [pip](https://pypi.python.org/pypi/pip)

### OS X Setup

To install Node.js and npm on Mac OS X, make sure that you have [Homebrew](http://brew.sh/)
installed and run:

```bash
$ brew install nvm
$ nvm install node
$ curl -L https://www.npmjs.com/install.sh | sh
```

Once this has completed:

```bash
$ cd website
$ make setup
```

This will install Hugo, Gulp, and all of the necessary Gulp plugins and build
the static assets for the site.

### Other Operating Systems Setup

Although the documentation is currently set up to be built and run on OS X, it's
also possible to do so on other systems. In addition to Node.js and npm you will
also need to [install Hugo](https://github.com/spf13/hugo/releases). Once those
are installed:

1. Navigate to the `website` folder
2. Run `npm install`
3. Run `npm run build` (this will build all of the necessary static assets, i.e.
   CSS, Javascript, etc.)

## Building the Docs Locally

To build the docs locally:

```bash
$ make site
```

This will generate a full build of the docs in the `public` folder, checking all links. If broken
links are found, see `linkchecker-errors.csv`.

## Running the Site Locally

To serve the site locally:

```bash
$ make serve
```

This will run the docs locally on `localhost:1313`. Navigate to
[localhost:1313/heron](http://localhost:1313/heron) to see the served docs. Or open the
browser from the command line:

```bash
$ open http://localhost:1313/heron
```

## Checking Links

To verify that the links in the docs are all valid, run `make linkchecker`, which will produce a
report of broken links. If `linkchecker` fails to install or run properly, you can install it manually.
Note that due to this [https://github.com/wummel/linkchecker/pull/657](issue), `linkchecker`
versions 9.2 and 9.3 require the python `requests` >= 2.2.0 and < 2.10.0.

```bash
$ pip uninstall requests
$ pip install requests==2.9.0
$ pip install linkchecker
```

## Publishing the Site

The content on the [twitter.github.io/heron](http://twitter.github.io/heron) website is what is
committed on the [gh-pages branch](https://github.com/twitter/heron/tree/gh-pages) of the heron repo.
To simplify publishing docs generated from `master` onto the `gh-pages` branch, the output directory
of the site build process (i.e., `website/public`) is a submodule that points to the `gh-pages` branch
of the heron repo. As a result you will notice that when you cd into gh-pages and run `git status`
or `git remote -v`, it appears as another heron repo based of the `gh-pages` branch.

```bash
$ git status
On branch master
Your branch is up-to-date with 'origin/master'.
$ cd website/public
$ git status
On branch gh-pages
Your branch is up-to-date with 'origin/gh-pages'.
```

To publish the site docs:

1. Make the site as described in the above section. Verify all links are valid.
2. Change to the `website/public` directory, commit and push to the `gh-pages` branch.
