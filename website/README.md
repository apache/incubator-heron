# Guide to the Heron Documentation

Heron's documentation was built using the following components:

* [Hugo](http://gohugo.io) --- Static site generator
* [GulpJS](http://gulpjs.com) --- Build tool for static assets
* [Twitter Bootstrap](http://getbootstrap.com) --- CSS and JavaScript

## Setup

Be sure you have a recent version of Node installed:

```bash
$ brew install nvm
$ nvm install node
```

If you have [Homebrew](http://brew.sh) and [npm](https://www.npmjs.com)
installed:

```bash
$ cd /path/to/heron/website
$ make setup
```

This will install Hugo, Gulp, and all necessary Gulp plugins.

## Running the Docs Locally

```bash
$ hugo server --watch
```

This will run the doc server locally on [localhost:1313](http://localhost:1313).

## Working with Static Assets

To build a full static asset distribution (CSS, JavaScript, fonts, and images):

```bash
$ gulp build
```

To work on assets in "watch" mode:

```bash
$ gulp dev
```

To publish website using github pages (requires python pip)

```bash
$ git checkout -b gh-pages
$ make site
```
If linkchecker errors are 0, commit then push to branch gh-pages

```bash
$ git push origin gh-pages
```
Submit PR to branch gh-pages (not master)



