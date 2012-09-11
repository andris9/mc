var config = require("./config.json"),
    amqp = require('amqp'),
    rabbit = amqp.createConnection({url: config.rabbit.url}),
    rabbitQueues = [],
    rabbitConnected = false,
    Stream = require('stream').Stream,
    utillib = require('util');

module.exports.createQueue = function(interfaceName, interfaceType){
    return new RabbitQueue(interfaceName, interfaceType);
}

function RabbitQueue(interfaceName, interfaceType){
    Stream.call(this);
    this.queue = [],
    this.interfaceName = interfaceName || "";
    this.interfaceType = (interfaceType || "exchange").toString().trim().toLowerCase();
    this.processing = false;
    this.interface = false;
    this.ctag = false;
    rabbitQueues.push(this);
    if(rabbitConnected){
        this.openInterface();
    }
}
utillib.inherits(RabbitQueue, Stream);

RabbitQueue.prototype.openInterface = function(){
    if(this.interfaceType == "exchange"){
        rabbit.exchange(this.interfaceName, {passive: true}, (function (exchange) {
            console.log('Exchange ' + exchange.name + ' is open');
            this.interface = exchange;
            this.process();
        }).bind(this));
    }else{
        var queueOptions = {};
        if(this.interfaceName){
            queueOptions = {passive: true};
        }else{
            queueOptions = {exclusive: true, durable: false, autoDelete: true};
        }
        rabbit.queue(this.interfaceName, queueOptions, (function(queue){
            queue.bind("mail.parsed", "#");
            console.log('Queue ' + queue.name + ' is open for subscription');
            this.interface = queue;
            this.interface.subscribe({ack: true}, this.subscription.bind(this)).addCallback((function(ok){
                this.ctag = ok.consumerTag;
            }).bind(this));
        }).bind(this));
    }
}

RabbitQueue.prototype.subscription = function(message, headers, deliveryInfo){
    console.log("Received message from " + this.interfaceName, JSON.stringify({message: message, envelope: deliveryInfo}));
    this.emit("message", message, headers, deliveryInfo);
}

RabbitQueue.prototype.shift = function(){
    if(this.interface && this.interface.shift){
        this.interface.shift();
    }
}

RabbitQueue.prototype.process = function(forced){
    if(this.processing && !forced){
        return;
    }

    if(!this.interface || !this.queue.length){
        this.processing = false;
        return;
    }
    this.processing = true;

    var message = this.queue.shift();
    if(!message){
        return process.nextTick(this.process.bind(this, true));
    }

    this.interface.publish(message[0] || "mail", JSON.stringify(message[1]), {contentType: 'application/json', deliveryMode: 2});

    process.nextTick(this.process.bind(this, true));
}

RabbitQueue.prototype.publish = function(key, data){
    if(!data && typeof key == "object"){
        data = key;
        key = undefined;
    }
    console.log("Publish new message", JSON.stringify(data))
    this.queue.push([key, data]);
    this.process();
}

rabbit.on("ready", function(){
    rabbitConnected = true;
    rabbitReconnectCounter = 0;

    console.log("Connected to rabbit");

    rabbitQueues.forEach(function(queue){
        if(rabbitConnected){
            queue.openInterface();
        }
    });
});

rabbit.on("close", function () {
    rabbitConnected = false;
    rabbitQueues.forEach(function(queue){
        if(queue.ctag){
            queue.interface.unsubscribe(queue.ctag);
        }
        queue.interface = false;
    });
    
    console.log("Connection to rabbit closed");

    setTimeout(function(){
        console.log("Reconnecting to rabbit");
        rabbit = amqp.createConnection({url: config.rabbit.url});
    }, (++rabbitReconnectCounter)*1000); 
});

rabbit.on("error", function(err){
    console.log(err.message || err, err.stack || "");
});