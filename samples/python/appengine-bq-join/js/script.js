var interval;
var queries = {
  'join': "SELECT wiki_title.title FROM (SELECT title FROM publicdata:samples.wikipedia WHERE timestamp > 1262304000) AS wiki_title JOIN (SELECT REGEXP_REPLACE(word, r'\W', '') word, COUNT(*) FROM publicdata:samples.shakespeare WHERE LENGTH(word) > 15 GROUP BY word IGNORE CASE) AS shakespeare_word ON wiki_title.title = shakespeare_word.word GROUP BY wiki_title.title LIMIT 10;"
 };

$(document).ready(function() {
  initialize();
});

function initialize() {
  var queriesElement = document.getElementById('queries');
  for (var queryItem in queries) {
    var queryElement = document.createElement('p');
    var queryLink = document.createElement('a');
    queryLink.className = 'query';
    queryLink.id = queryItem;
    queryLink.innerHTML = queries[queryItem];
    queryElement.appendChild(queryLink);
    queriesElement.appendChild(queryElement);
  }
  $('.query').click(function() {
    var queryKey = this.id;
    query(queries[queryKey]);
  });
}

function query(query) {
  $.ajax({
    url: '/query',
    type: 'POST',
    dataType: 'json',
    data: 'query=' + query,
    success: function(data) {
      var jobId = data['jobId'];
      setTimeout('pollJob("' + jobId + '")', 1000);
    },
    error: function(request, status, error) {
      alert(error);
    }
  });
}

function pollJob(jobId) {
  $.ajax({
    url: '/query?jobId=' + jobId,
    success: function(data) {
      var complete = data['jobComplete'];
      if (complete) {
        if (interval) {
          clearInterval(interval);
        }
        parseResults(data);
      } else {
        if (!interval) {
          interval = setInterval('pollJob("' + jobId + '")', 1000);
        }
      }
    },
    error: function(request, status, error) {
      alert(error);
    }
  });
}

function parseResults(data) {
  var columns = data['schema']['fields'];
  var displayColumns = [];
  for (var column in columns) {
    var columnName = columns[column]['name'];
    displayColumns.push(columnName);
  }

  var rows = data['formattedRows'];
  var displayRows = [];
  for (var row in rows) {
    var rowData = rows[row].join(', ');
    displayRows.push(rowData);
  }

  var text = displayColumns.join(', ');
  text += '<br>';
  text += displayRows.join('<br>');

  document.getElementById('results').innerHTML = text;
}
