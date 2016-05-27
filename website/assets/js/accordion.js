$(document).ready(function() {
  var LS = window.localStorage;
  var selected = LS['sidebar-selected'] || null;

  if (selected != null) {
    var id = '#collapse-' + selected;

    $(id).collapse('show');

    $(id).on('hide.bs.collapse', function() {
      LS['sidebar-selected'] = null;
    });
  }

  $('.panel-heading').click(function() {
    LS['sidebar-selected'] = this['id'];
  });
});
