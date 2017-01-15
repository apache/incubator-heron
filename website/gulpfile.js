var gulp = require('gulp'),
    // Pulls in any Gulp-related metadata
    pkg = require('./package.json'),
    // Use $ to invoke Gulp plugins
    $ = require('gulp-load-plugins')(),
    // All non-Gulp modules here
    del = require('del');

// Define the source and distribution directories
var srcDir = './assets';
var distDir = './static';

// Define asset sources
var SRC = {
  js: srcDir + '/js/**/*.js',
  css: srcDir + '/css/**/*.css',
  sass: srcDir + '/sass/**/*.scss',
  fonts: srcDir + '/fonts/**/*',
  images: srcDir + '/img/**/*',
  javadoc: './api/**/*'
}

// Define asset distribution destination
var DIST = {
  css: distDir + '/css',
  js: distDir + '/js',
  fonts: distDir + '/fonts',
  images: distDir + '/img',
  javadoc: './public',
  all: distDir
}

// JavaScript assets
gulp.task('js', function() {
  return gulp.src(SRC.js)
    .pipe($.uglify().on('error', function(err) { console.log(err); }))
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

// Javadoc
gulp.task('javadoc', function() {
  gulp.src(SRC.javadoc)
    .pipe(gulp.dest(DIST.javadoc));
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

gulp.task('images:watch', function() {
  gulp.watch(SRC.images, ['images']);
});

// One-time build; doesn't watch for changes
gulp.task('build', ['js', 'sass', 'css', 'javadoc', 'fonts', 'images']);

// Delete static folder
gulp.task('clean', function() {
  del(DIST.all);
});

// Run in development (i.e. watch) mode
gulp.task('dev', ['build', 'js:watch', 'sass:watch', 'css:watch', 'images:watch']);

// Help => list tasks
gulp.task('help', $.taskListing.withFilters(null, 'help'));

// Default
gulp.task('default', ['help']);
