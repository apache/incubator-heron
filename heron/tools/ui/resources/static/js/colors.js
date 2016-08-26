/**
 * Code for handling color-scheme changes
 */

(function () {
  function toggleColors () {
    localStorage.colors = localStorage.colors === 'colorblind' ? 'default' : 'colorblind';
    window.location.reload();
  }

  d3.selectAll('.color-choice button').on('click', toggleColors);
  d3.selectAll('.color-choice button').classed('active', function () {
    return d3.select(this).attr('data-color') === (localStorage.colors || 'default');
  });
}());
