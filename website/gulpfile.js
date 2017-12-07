var gulp = require('gulp'),
    // Pulls in any Gulp-related metadata
    pkg = require('./package.json'),
    hash = require('gulp-hash'),
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
  sass: srcDir + '/sass/**/*.scss'
}

// Define asset distribution destination
var DIST = {
  js: distDir + '/js',
  css: distDir + '/css',
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

gulp.task('js-dev', function(done) {
  del(['static/js/app*.js']);

  gulp.src(SRC.js)
    .pipe(hash())
    .pipe($.uglify().on('error', function(err) { console.log(err); }))
    .pipe(gulp.dest(DIST.js))
    .pipe(hash.manifest('hash.json'))
    .pipe(gulp.dest('data/assets/js'));
  done();
});

gulp.task('js:watch', function() {
  gulp.watch(SRC.js, gulp.series('js-dev'));
});

// Sass assets (dev mode)
gulp.task('sass-dev', function(done) {
  del(['static/css/style*.css']);

  gulp.src(SRC.sass)
    .pipe($.sass({
      outputStyle: 'compressed'
    }).on('error', function(err) { $.sass.logError; }))
    .pipe(hash())
    .pipe($.autoprefixer({
      browsers: ['last 2 versions'],
      cascade: false
    }))
    .pipe($.cleanCss())
    .pipe(gulp.dest(DIST.css))
    .pipe(hash.manifest('hash.json'))
    .pipe(gulp.dest('data/assets/css'));
  done();
});

// Sass assets
gulp.task('sass', function(done) {
  del(['static/css/style*.css']);

  gulp.src(SRC.sass)
    .pipe($.sass({
      outputStyle: 'compressed'
    }).on('error', function(err) { $.sass.logError; }))
    .pipe($.autoprefixer({
      browsers: ['last 2 versions'],
      cascade: true
    }))
    .pipe($.cleanCss())
    .pipe(gulp.dest(DIST.css))
    .pipe(gulp.dest('data/assets/css'));
  done();
});

gulp.task('sass:watch', function() {
  gulp.watch(SRC.sass, gulp.series('sass-dev'));
});

// One-time build; doesn't watch for changes
gulp.task('build', gulp.series('js', 'sass'));

// Run in development (i.e. watch) mode
gulp.task('dev', gulp.series('js-dev', 'sass-dev', gulp.parallel('js:watch', 'sass:watch')));

// Help => list tasks
gulp.task('help', function(done) {
  $.taskListing.withFilters(null, 'help')
  done();
});

// Default
gulp.task('default', gulp.series('help'));
