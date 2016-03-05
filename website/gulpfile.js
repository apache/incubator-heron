var gulp = require('gulp'),
    // Pulls in any Gulp-related metadata
    pkg = require('./package.json'),
    // Use $ to invoke Gulp plugins
    $ = require('gulp-load-plugins')(),
    // All non-Gulp modules here
    del = require('del');

// Define the source and distribution directories
var srcDir = './src';
var distDir = './static';

// Define asset sources
var SRC = {
  js: srcDir + '/js/**/*.js',
  css: srcDir + '/css/**/*.css',
  sass: srcDir + '/sass/**/*.scss',
  fonts: srcDir + '/fonts/**/*',
  images: srcDir + '/img/**/*'
  //apiDocs: './api/rest-api.md'
}

// Define asset distribution destination
var DIST = {
  css: distDir + '/css',
  js: distDir + '/js',
  fonts: distDir + '/fonts',
  images: distDir + '/img',
  //apiDocs: distDir + '/api/rest-api.html',
  all: distDir
}

// JavaScript assets
gulp.task('js', function() {
  return gulp.src(SRC.js)
    .pipe($.uglify())
    .pipe($.concat('app.min.js'))
    .pipe(gulp.dest(DIST.js));
});

gulp.task('js:watch', function() {
  gulp.watch(SRC.js, ['js']);
});

// CSS assets
gulp.task('css', function() {
  return gulp.src(SRC.css)
    .pipe(gulp.dest(DIST.css));
});

gulp.task('css:watch', function() {
  return gulp.watch(SRC.css, ['css']);
});

// Sass assets
gulp.task('sass', function() {
  gulp.src(SRC.sass)
    .pipe($.sass().on('error', function(err) { console.log(err); }))
    .pipe($.cleanCss())
    .pipe($.concat('style.min.css'))
    .pipe(gulp.dest(DIST.css));
});

gulp.task('sass:watch', function() {
  gulp.watch(SRC.sass, ['sass']);
});

// Fonts
gulp.task('fonts', function() {
  gulp.src(SRC.fonts)
    .pipe(gulp.dest(DIST.fonts));
});

// Images
gulp.task('images', function() {
  gulp.src(SRC.images)
    .pipe(gulp.dest(DIST.images));
});

// Generate API docs using Aglio
gulp.task('api-docs', function() {
  gulp.src(DIST.apiDocs)
    .pipe($.aglio({template: 'default'}))
    .pipe(gulp.dest('.'));
});

// One-time build; doesn't watch for changes
gulp.task('build', ['js', 'sass', 'css', 'fonts', 'images']);

// Delete static folder
gulp.task('clean', function() {
  del(DIST.all);
});

// Run in development (i.e. watch) mode
gulp.task('dev', ['build', 'api-docs', 'js:watch', 'sass:watch', 'css:watch']);

// Help => list tasks
gulp.task('help', $.taskListing.withFilters(null, 'help'));

// Default
gulp.task('default', ['help']);
