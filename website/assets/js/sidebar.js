var LS = window.localStorage;
var selected = LS['sidebar-selected'] || null;

if (selected != null) {
  $('#collapse-' + selected).collapse('show');
}

$('.panel-heading').click(function() {
  LS['sidebar-selected'] = this['id'];
});
