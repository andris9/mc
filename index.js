var amqp = require("amqp"),
    config = require("./config.json"),
    rabbit = require("./rabbit"),
    rabbitQueue = rabbit.createQueue("", "queue"),
    http = require("http"),
    urllib = require("url");

rabbitQueue.on("message", function(message, callback){
    var url = urllib.parse(config.targetUrl, true, true);

    url.method = "POST";
    url.headers = {
        "Content-Type": "application/json"
    };

    var req = http.request(url, function(res) {
        console.log('STATUS: ' + res.statusCode);
        console.log('HEADERS: ' + JSON.stringify(res.headers));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            console.log('BODY: ' + chunk);
        });
        res.on("end", function(){
            console.log("Request done");
            rabbitQueue.shift();
        });
    });

    req.on('error', function(e) {
        console.log('problem with request: ' + e.message);
    });

    // write data to request body
    req.write(JSON.stringify(message));
    req.end();

});

process.on("SIGTERM", function(){
    console.log("Exited on SIGTERM");
    process.exit(0);
});

process.on("SIGINT", function(){
    console.log("Exited on SIGINT");
    process.exit(0);    
});

process.on("uncaughtException", function(err) {
    console.log(err.stack);
    process.exit(1);
});

var url = urllib.parse(config.targetUrl);