const gulp     = require("gulp"),
      sass     = require("gulp-sass"),
      hash     = require("gulp-hash"),
      prefixer = require("gulp-autoprefixer"),
      uglify   = require("gulp-uglify"),
      del      = require("del");

const SRCS = {
  sass: 'assets/sass/**/*.sass',
  js: 'assets/js/app.js',
  hash: 'hash.json'
}

const DIST = {
  css: 'dist/css',
  js: 'dist/js'
}

const sassConfig = {
  outputStyle: 'compressed'
}

const prefixerConfig = {
  browsers: ['last 2 versions'],
	cascade: false
}

gulp.task('sass', (done) => {
  del(['static/css/style-*.css']);

  gulp.src(SRCS.sass)
    .pipe(sass({
      outputStyle: 'compressed'
    }).on('error', sass.logError))
    .pipe(prefixer(prefixerConfig))
    .pipe(gulp.dest(DIST.css));
  done();
});

gulp.task('sass-dev', (done) => {
  del(['static/css/style-*.css']);

  gulp.src(SRCS.sass)
    .pipe(sass(sassConfig).on('error', sass.logError))
    .pipe(hash())
    .pipe(prefixer(prefixerConfig))
    .pipe(gulp.dest(DIST.css))
    .pipe(hash.manifest(DIST.hash))
    .pipe(gulp.dest(DIST.css));
  done();
});

gulp.task('sass:watch', () => {
  gulp.watch(SRCS.sass, gulp.series('sass-dev'));
});

gulp.task('js', (done) => {
  gulp.src(SRCS.js)
    .pipe(gulp.dest(DIST.js));

  done();
});

gulp.task('js-dev', (done) => {
  del(['static/js/app-*.js']);

  gulp.src(SRCS.js)
    .pipe(hash())
    .pipe(gulp.dest(DIST.js));
  done();
});

gulp.task('js:watch', () => {
  gulp.watch(SRCS.js, gulp.series('js-dev'));
});

gulp.task('build', gulp.series('sass', 'js'));

gulp.task('dev', gulp.series('sass-dev', 'js-dev', gulp.parallel('sass:watch', 'js:watch')));
