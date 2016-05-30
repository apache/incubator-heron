$(document).ready(function() {
  var LS = window.localStorage;
  var selected = LS['sidebar-selected'] || null;
  console.log("Selected: " + selected);

  if (selected != null) {
    $('#' + selected).collapse('show');
  }

  var tabs = $("[id^='collapse']");
  tabs.on('shown.bs.collapse', function() {
    LS['sidebar-selected'] = this.id;
  });

  tabs.on('hidden.bs.collapse', function() {
    LS['sidebar-selected'] = null;
  });
});
