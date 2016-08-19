/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A jQuery plugin for PAging and taILing data (i.e., a
// 'PAILer'). Paging occurs when scrolling reaches the "top" and
// tailing occurs when scrolling has reached the "bottom".

// A 'read' function must be provided for reading the data (in
// bytes). This function should expect an "options" object with the
// fields 'offset' and 'length' set for reading the data. The result
// from of the function should be a "promise" like value which has a
// 'success' and 'error' callback which each take a function. An
// object with at least two fields defined ('offset' and 'length') is
// expected on success. A third field, 'data' may be provided if
// 'length' is greater than 0. If the offset requested is greater than
// the available offset, the result should be an object with the
// 'offset' field set to the available offset (i.e., the length of the
// data) and the 'length' field set to 0.

// The plugin prepends, appends, and updates the "html" component of
// the elements specified in the jQuery selector (e.g., doing
// $('#data').pailer(...) means that data will be updated within
// $('#data') via $('#data').prepend(...) and $('#data').append(...)
// and $('#data').html(...) calls).

// An indicator paragraph element (i.e., <p>) can be specified that
// the plugin will write text to describing any status/errors that
// have been encountered.

// Data will automagically get truncated at some specified length,
// configurable via the 'truncate-length' option. Likewise, the amount
// of data paged in at a time can be configured via the 'page-size'
// option.

// Example:
//   HTML:
//     <div id="data" style="white-space:pre-wrap;"></div>
//
//     <div style="position: absolute; left: 5px; top: 0px;">
//       <p id="indicator"></p>
//     </div>
//  Javascript:
//    $('#data').pailer({
//      'read': function(options) {
//        var settings = $.extend({
//          'offset': -1,
//          'length': -1
//        }, options);
//        var url = '/url/for/data'
//          + '?offset=' + settings.offset
//          + '&length=' + settings.length;
//        return $.getJSON(url);
//      },
//      'indicator': $('#indicator')
//    });

(function($) {
  function Pailer(read, element, indicator, page_size, truncate_length) {
    var this_ = this;

    this_.read = read;
    this_.element = element;
    this_.indicator = indicator;
    this_.initialized = false;
    this_.start = -1;
    this_.end = -1;
    this_.paging = false;
    this_.tailing = true;

    page_size || $.error('Expecting page_size to be defined');
    truncate_length || $.error('Expecting truncate_length to be defined');

    this_.page_size = page_size;
    this_.truncate_length = truncate_length;

    this_.element.css('overflow', 'auto');

    this_.element.scroll(function () {
      var scrollTop = this_.element.scrollTop();
      var height = this_.element.height();
      var scrollHeight = this_.element[0].scrollHeight;

      if (scrollTop == 0) {
        this_.page();
      } else if (scrollTop + height >= scrollHeight) {
        if (!this_.tailing) {
          this_.tailing = true;
          this_.tail();
        }
      } else {
        this_.tailing = false;
      }
    });
  }


  Pailer.prototype.initialize = function() {
    var this_ = this;

    // Set an indicator while we load the data.
    this_.indicate('(LOADING)');

    this_.read({'offset': -1})
      .success(function(data) {
        this_.indicate('');

        // Get the last page of data.
        if (data.offset > this_.page_size) {
          this_.start = this_.end = data.offset - this_.page_size;
        } else {
          this_.start = this_.end = 0;
        }

        this_.initialized = true;
        this_.element.html('');
        setTimeout(function() { this_.tail(); }, 0);
      })
      .error(function() {
        this_.indicate('(FAILED TO INITIALIZE ... RETRYING)');
        setTimeout(function() {
          this_.indicate('');
          this_.initialize();
        }, 1000);
      });
  }


  Pailer.prototype.page = function() {
    var this_ = this;

    if (!this_.initialized) {
      return;
    }

    if (this_.paging) {
      return;
    }

    this_.paging = true;
    this_.indicate('(PAGING)');

    if (this_.start == 0) {
      this_.paging = false;
      this_.indicate('(AT BEGINNING OF FILE)');
      setTimeout(function() { this_.indicate(''); }, 1000);
      return;
    }

    var offset = this_.start - this_.page_size;
    var length = this_.page_size;

    if (offset < 0) {
      offset = 0;
      length = this_.start;
    }

    // Buffer the data in case what gets read is less than 'length'.
    var buffer = '';

    var read = function(offset, length) {
      this_.read({'offset': offset, 'length': length})
        .success(function(data) {
          if (data.length < length) {
              buffer += data.data;
              read(offset + data.length, length - data.length);
          } else if (data.length > 0) {
            this_.indicate('(PAGED)');
            setTimeout(function() { this_.indicate(''); }, 1000);

            // Prepend buffer onto data.
            data.offset -= buffer.length;
            data.data = buffer + data.data;
            data.length = data.data.length;

            // Truncate to the first newline (unless this is the beginning).
            if (data.offset != 0) {
              var index = data.data.indexOf('\n') + 1;
              data.offset += index;
              data.data = data.data.substring(index);
              data.length -= index;
            }

            this_.start = data.offset;

            var scrollTop = this_.element.scrollTop();
            var scrollHeight = this_.element[0].scrollHeight;

            this_.element.prepend(data.data);

            scrollTop += this_.element[0].scrollHeight - scrollHeight;
            this_.element.scrollTop(scrollTop);

            this_.paging = false;
          }
        })
        .error(function() {
          this_.indicate('(FAILED TO PAGE ... RETRYING)');
          setTimeout(function() {
            this_.indicate('');
            this_.page();
          }, 1000);
        });
    }

    read(offset, length);
  }


  Pailer.prototype.tail = function() {
    var this_ = this;

    if (!this_.initialized) {
      return;
    }

    this_.read({'offset': this_.end, 'length': this_.truncate_length})
      .success(function(data) {
        var scrollTop = this_.element.scrollTop();
        var height = this_.element.height();
        var scrollHeight = this_.element[0].scrollHeight;

        // Check if we are still at the bottom (since this event might
        // have fired before the scroll event has been dispatched).
        if (scrollTop + height < scrollHeight) {
          this_.tailing = false;
          return;
        }

        if (data.length > 0) {
          // Truncate to the first newline if this is the first time
          // (and we aren't reading from the beginning of the log).
          if (this_.start == this_.end && data.offset != 0) {
            var index = data.data.indexOf('\n') + 1;
            data.offset += index;
            data.data = data.data.substring(index);
            data.length -= index;
            this_.start = data.offset; // Adjust the actual start too!
          }

          this_.end = data.offset + data.length;

          this_.element.append(data.data);

          scrollTop += this_.element[0].scrollHeight - scrollHeight;
          this_.element.scrollTop(scrollTop);

          // Also, only if we're at the bottom, truncate data so that we
          // don't consume too much memory. TODO(benh): Only do
          // truncations if we've been at the bottom for a while.
          this_.truncate();
        }

        // Tail immediately if we got as much data as requested (since
        // this probably means we've waited around a while). The
        // alternative here would be to not get data in chunks, but the
        // potential issue here is that we might end up requesting GB of
        // log data at a time ... the right solution here might be to do
        // a request to determine the new ending offset and then request
        // the proper length.
        if (data.length == this_.truncate_length) {
          setTimeout(function() { this_.tail(); }, 0);
        } else {
          setTimeout(function() { this_.tail(); }, 1000);
        }
      })
      .error(function() {
        this_.indicate('(FAILED TO TAIL ... RETRYING)');
        this_.initialized = false;
        setTimeout(function() {
          this_.indicate('');
          this_.initialize();
        }, 1000);
      });
  }


  Pailer.prototype.indicate = function(text) {
    var this_ = this;

    if (this_.indicator) {
      this_.indicator.text(text);
    }
  }


  Pailer.prototype.truncate = function() {
    var this_ = this;

    var length = this_.element.html().length;
    if (length >= this_.truncate_length) {
      var index = length - this_.truncate_length;
      this_.start = this_.end - this_.truncate_length;
      this_.element.html(this_.element.html().substring(index));
    }
  }

  $.fn.pailer = function(options) {
    var settings = $.extend({
      'read': function() { return {
        'success': function() {},
        'error': function(f) { f(); }
      }},
      'page_size': 8 * 4096, // 8 "pages".
      'truncate_length': 50000
    }, options);

    this.each(function() {
      var pailer = $.data(this, 'pailer');
      if (!pailer) {
        pailer = new Pailer(settings.read,
                            $(this),
                            settings.indicator,
                            settings.page_size,
                            settings.truncate_length);
        $.data(this, 'pailer', pailer);
        pailer.initialize();
      }
    });
  }
})(jQuery);
