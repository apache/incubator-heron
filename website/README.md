# Heron Documentation

Heron's documentation was primarily built using the following components:

* [Hugo](http://gohugo.io) --- Static site generator
* [GulpJS](http://gulpjs.com) --- Build tool for static assets
* [Twitter Bootstrap](http://getbootstrap.com) --- CSS and JavaScript

## Documentation Setup

Running the Heron documentation locally requires that you have the following
installed:

* [Make](https://www.gnu.org/software/make/)
* [Node.js](https://nodejs.org/en/)
* [npm](https://www.npmjs.com/)

### OS X Setup

To install Node.js and npm on Mac OS X, make sure that you have
[Homebrew](http://brew.sh/) installed and run:

```bash
$ brew install nvm
$ nvm install node
```

Once this has completed:

```bash
# Within the /heron directory
$ cd website
$ make setup
```

This will install Hugo, Gulp, and all of the necessary Gulp plugins and build
the static assets for the site.

### Other Operating Systems

Although the documentation is currently set up to be built and run on OS X, it's
also possible to do so on other systems. In addition to Node.js and npm you will
also need to [install Hugo](https://github.com/spf13/hugo/releases). Once those
are installed:

1. Navigate to the `website` folder
2. Run `npm install`
3. Run `npm run build` (this will build all of the necessary static assets, i.e.
   CSS, Javascript, etc.)

Now you can run the docs locally. For more info, see the section directly below.

## Building the Docs Locally

To build the docs:

```bash
$ make site
```

This will generate a full build of the docs in the `public` folder. To serve
the docs locally, see the section directly below.

## Running the Docs Locally

```bash
$ make serve
```

This will run the docs locally on `localhost` port 1313. Navigate to
`localhost:1313/heron` in your browser to see the served docs. Or open the
browser from the command line:

```bash
$ open http://localhost:1313/heron
```

To make site, including linkchecker.  If broken links found by linkchecker, see linkchecker-errors.csv

```bash
$ make site
```




