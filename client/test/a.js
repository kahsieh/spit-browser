


const url='http://' + IP + ':' + PORT;

function getAjax(url, success) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', url);
    xhr.onreadystatechange = function() {
        if (xhr.readyState>3 && xhr.status==200) success(xhr.responseText);
    };
    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
    xhr.send();
    return xhr;
}

function postAjax(url, data, success) {
    var params = typeof data == 'string' ? data : Object.keys(data).map(
            function(k){ return encodeURIComponent(k) + '=' + encodeURIComponent(data[k]) }
        ).join('&');

    var xhr =  new XMLHttpRequest();
    xhr.open('POST', url);
    xhr.onreadystatechange = function() {
        if (xhr.readyState>3 && xhr.status==200) { success(xhr.responseText); }
    };
    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    xhr.send(params);
    return xhr;
}

function get_data() {
  getAjax(url + '/send', function(data) {
    var json = JSON.parse(data);
    console.log(json);
    self.postMessage(json['data'])
  });
}

function send_answer() {
  onmessage = function(e) {
          for (const msg of e.data){
            postAjax(url+'/receive' , msg, function(data){ console.log(data); })
          }
  }
}
setInterval(get_data, 1000)
setInterval(send_answer, 1000)
