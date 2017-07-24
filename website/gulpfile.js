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
  images: srcDir + '/img/**/*'
}

// Define asset distribution destination
var DIST = {
  css: distDir + '/css',
  js: distDir + '/js',
  fonts: distDir + '/fonts',
  images: distDir + '/img',
  all: distDir
}

// JavaScript assets
gulp.task('js', function(done) {
  gulp.src(SRC.js)
    .pipe($.uglify().on('error', function(err) { console.log(err); }))
    .pipe($.concat('app.min.js'))
    .pipe(gulp.dest(DIST.js));
  done();
});

gulp.task('js:watch', function() {
  gulp.watch(SRC.js, gulp.series('js'));
});

// CSS assets
gulp.task('css', function(done) {
  gulp.src(SRC.css)
    .pipe(gulp.dest(DIST.css));
  done();
});

gulp.task('css:watch', function() {
  return gulp.watch(SRC.css, gulp.watch('css'));
});

// Sass assets
gulp.task('sass', function(done) {
  gulp.src(SRC.sass)
    .pipe($.sass().on('error', function(err) { console.log(err); }))
    .pipe($.cleanCss())
    .pipe($.concat('style.min.css'))
    .pipe(gulp.dest(DIST.css));
  done();
});

gulp.task('sass:watch', function() {
  gulp.watch(SRC.sass, gulp.series('sass'));
});

// Fonts
gulp.task('fonts', function(done) {
  gulp.src(SRC.fonts)
    .pipe(gulp.dest(DIST.fonts));
  done();
});

// Images
gulp.task('images', function(done) {
  gulp.src(SRC.images)
    .pipe(gulp.dest(DIST.images));
  done();
});

gulp.task('images:watch', function() {
  gulp.watch(SRC.images, gulp.series('images'));
});

// One-time build; doesn't watch for changes
gulp.task('build', gulp.series('js', 'sass', 'css', 'fonts', 'images'));

// Delete static folder
gulp.task('clean', function(done) {
  del(DIST.all);
  done();
});

// Run in development (i.e. watch) mode
gulp.task('dev', gulp.series('build', gulp.parallel('js:watch', 'sass:watch', 'css:watch', 'images:watch')));

// Help => list tasks
gulp.task('help', function(done) {
  $.taskListing.withFilters(null, 'help')
  done();
});

// Default
gulp.task('default', gulp.series('help'));
