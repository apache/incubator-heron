$(document).ready(function() {
  var LS = window.localStorage;
  var selected = LS['sidebar-selected'] || null;

  if (selected != null) {
    var selectedElement = '#' + selected;
    $(selectedElement).collapse('show');
    $('aside.hn-sidebar').scrollTo(selectedElement);
  }

  var tabs = $("[id^='collapse']");
  tabs.on('shown.bs.collapse', function() {
    LS['sidebar-selected'] = this.id;
  });

  tabs.on('hidden.bs.collapse', function() {
    LS['sidebar-selected'] = null;
  });
});
