window.addEventListener('load', function() {


  const community = document.querySelector("a[href='#community']").parentNode;
  const communityMenu =
    '<li>' +
    '<a id="community-menu" href="#">Community<span style="font-size: 0.75em">&nbsp;▼</span></a>' +
    '<div id="community-dropdown" class="hide" >' +
      '<ul id="community-dropdown-items">' +
        '<li><a href="/contact" style="color:#1d3f5f">Contact</a></li>' +
        '<li><a href="/events" style="color:#1d3f5f">Events</a></li>' +
        '<li><a href="https://github.com/apache/incubator-heron/issues" style="color:#1d3f5f">Issue tracking</a></li>' +
        '<li><a href="/resources" style="color:#1d3f5f">Resources</a></li>' +
        '<li><a href="/team" style="color:#1d3f5f">Team</a></li>' +
      '</ul>' +
    '</div>' +
    '</li>';
    
  community.innerHTML = communityMenu;
  const communityMenuItem = document.getElementById("community-menu");
  const communityDropDown = document.getElementById("community-dropdown");
  communityMenuItem.addEventListener("click", function(event) {
    event.preventDefault();
    if (communityDropDown.className == 'hide') {
      communityDropDown.className = 'visible';
    } else {
      communityDropDown.className = 'hide';
    }
  });

const apache = document.querySelector("a[href='#apache']").parentNode;
const apacheMenu =
  '<li>' +
  '<a id="apache-menu" href="#">Apache <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
  '<div id="apache-dropdown" class="hide">' +
    '<ul id="apache-dropdown-items">' +
      '<li><a href="https://www.apache.org/" target="_blank" style="color:#1d3f5f" >Foundation &#x2750</a></li>' +
      '<li><a href="https://www.apache.org/licenses/" target="_blank" style="color:#1d3f5f">License &#x2750</a></li>' +
      '<li><a href="https://www.apache.org/foundation/sponsorship.html" target="_blank" style="color:#1d3f5f">Sponsorship &#x2750</a></li>' +
      '<li><a href="https://www.apache.org/foundation/thanks.html" target="_blank" style="color:#1d3f5f">Thanks &#x2750</a></li>' +
      '<li><a href="https://www.apache.org/security" target="_blank" style="color:#1d3f5f">Security &#x2750</a></li>' +
    '</ul>' +
  '</div>' +
  '</li>';

  apache.innerHTML = apacheMenu;

  const apacheMenuItem = document.getElementById("apache-menu");
  const apacheDropDown = document.getElementById("apache-dropdown");
  apacheMenuItem.addEventListener("click", function(event) {
    event.preventDefault();

    if (apacheDropDown.className == 'hide') {
      apacheDropDown.className = 'visible';
    } else {
      apacheDropDown.className = 'hide';
    }
  });
});