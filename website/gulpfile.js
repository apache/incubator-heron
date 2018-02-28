var gulp     = require("gulp"),
    sass     = require("gulp-sass"),
    hash     = require("gulp-hash"),
    prefixer = require("gulp-autoprefixer"),
    del      = require("del");

var SRCS = {
  sass: 'assets/sass/**/*.sass',
  js: 'assets/js/app.js'
}

gulp.task('sass', (done) => {
  del(['static/css/style-*.css']);

  gulp.src(SRCS.sass)
    .pipe(sass({
      outputStyle: 'compressed'
    }).on('error', sass.logError))
    .pipe(prefixer({
			browsers: ['last 2 versions'],
			cascade: false
		}))
    .pipe(gulp.dest('static/css'));
  done();
});

gulp.task('sass-dev', (done) => {
  del(['static/css/style-*.css']);

  gulp.src(SRCS.sass)
    .pipe(sass({
      outputStyle: 'compressed'
    }).on('error', sass.logError))
    .pipe(hash())
    .pipe(prefixer({
			browsers: ['last 2 versions'],
			cascade: false
		}))
    .pipe(gulp.dest('static/css'))
    .pipe(hash.manifest('hash.json'))
    .pipe(gulp.dest('data/css'));
  done();
});

gulp.task('sass:watch', () => {
  gulp.watch(SRCS.sass, gulp.series('sass-dev'));
});

gulp.task('js', (done) => {
  gulp.src(SRCS.js)
    .pipe(gulp.dest('static/js'));

  done();
});

gulp.task('js-dev', (done) => {
  del(['static/js/app-*.js']);

  gulp.src(SRCS.js)
    .pipe(hash())
    .pipe(gulp.dest('static/js'))
    .pipe(hash.manifest('hash.json'))
    .pipe(gulp.dest('data/js'));

  done();
});

gulp.task('js:watch', () => {
  gulp.watch(SRCS.js, gulp.series('js-dev'));
});

gulp.task('build', gulp.series('sass', 'js'));

gulp.task('dev', gulp.series('sass-dev', 'js-dev', gulp.parallel('sass:watch', 'js:watch')));
