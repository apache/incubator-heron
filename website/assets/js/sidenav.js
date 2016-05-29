$(document).ready(function () {
  $("a[role='button']").click(function() {
      localStorage.setItem('selected', $(this).attr('href'));
  });

  var collapseItem = localStorage.getItem('selected');
  if (collapseItem) {
     $(collapseItem).collapse('show');
  }
});
