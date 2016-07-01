# Heron Documentation

Heron's documentation was primarily built using the following components:

* [Hugo](http://gohugo.io) --- Static site generator
* [GulpJS](http://gulpjs.com) --- Build tool for static assets
* [Twitter Bootstrap](http://getbootstrap.com) --- Sass/CSS and JavaScript

## Documentation Setup

Running the Heron documentation locally requires that you have the following
installed:

* [Make](https://www.gnu.org/software/make/)
* [Node.js](https://nodejs.org/en/)
* [npm](https://www.npmjs.com/)
* [pip](https://pypi.python.org/pypi/pip)

### OS X Setup

To install Node.js and npm on Mac OS X, make sure that you have
[Homebrew](http://brew.sh/) installed and run:

```bash
$ brew update && brew install nvm && source $(brew --prefix nvm)/nvm.sh
$ nvm install node
$ curl -L https://www.npmjs.com/install.sh | sh
```

Once this has completed:

```bash
$ cd website
$ make setup
$ make build-static-assets
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
3. Run `make build-static-assets` (this will build all of the necessary static
   assets, i.e. CSS, Javascript, etc.)

## Building the Docs Locally

To build the docs locally:

```bash
$ make site
```

This will generate a full build of the docs in the `public` folder, a full build
of the static assets in the `static` folder, and check all links. If broken
links are found, see `linkchecker-errors.csv` (you can safely leave this file in
your directory, as it is ignored by Git).

## Running the Site Locally

To serve the site locally:

```bash
$ make serve
```

This will run the docs locally on `localhost:1313`. Navigate to
[localhost:1313/heron](http://localhost:1313/heron) to see the served docs. Or
open the browser from the command line:

```bash
$ open http://localhost:1313/heron
```

## Working on Static Assets

If you'd like to work on the site's static assets (Sass/CSS, JavaScript, etc.),
you can run `make develop-static-assets`. This will build all of the static
assets in the `assets` folder, store the build artifacts in the `static`
folder, and then watch the `assets` folder for changes, rebuilding when changes
are made.

## Checking Links

To verify that the links in the docs are all valid, make sure `wget` is installed
and run `make linkchecker`, which will produce a report of broken links. However,
no URL of parent webpages that contain broken links will be reported, but
one can use `grep` command to find those parent webpages.

## Publishing the Site

The content on the [twitter.github.io/heron](http://twitter.github.io/heron)
website is what is committed on the [gh-pages
branch](https://github.com/twitter/heron/tree/gh-pages) of the Heron repo. To
simplify publishing docs generated from `master` onto the `gh-pages` branch, the
output directory of the site build process (i.e. `website/public`) is a
submodule that points to the `gh-pages` branch of the heron repo.

A one-time setup is required to initialize the `website/public` submodule:

```
$ rm -rf website/public
$ git submodule update --init
$ cd website/public
$ git checkout gh-pages
$ git remote rename origin upstream
```

With the submodule in place, you will notice that when you `cd` into `website/public`
and run `git status` or `git remote -v`, it appears as another heron repo based off
of the `gh-pages` branch.

```bash
$ git status
On branch master
Your branch is up-to-date with 'upstream/master'.
$ cd website/public
$ git status
On branch gh-pages
Your branch is up-to-date with 'upstream/gh-pages'.
```

To publish the site docs:

1. Make the site as described in the above section. Verify all links are valid.
2. Change to the `website/public` directory, commit everything to the `gh-pages` branch and push to
   the `upstream` repo. You can also push to the `gh-pages` branch of your own fork and verify the
   site at `http://[username].github.io/heron`.
