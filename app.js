
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require("fs");

var uuid = require('node-uuid');

var exec = require('child_process').exec;




app.use(bodyParser.json()); 
app.use(bodyParser.urlencoded({ extended: true }));




var kue = require('kue')
  , queue = kue.createQueue();





queue.process('heat_map',2, function(job, done){



var spark_path = "/home/cochung/spark_full" ;
  var command1 = '/home/cochung/spark_full/spark-1.4.1/bin/spark-submit --class sparkgis.SparkGISMain /home/cochung/spark_full/spark-gis/target/uber-spark-gis-1.0.jar';
  

  var job_title = job.data.title
  var cmd_params = job.data.cmd_params
  var log_cmd = " > "+job_title+".txt"


   exec(command1+ cmd_params+log_cmd,function(error, stdout, stderr){
    if (stdout.length > 0) console.log('stdout: ' + stdout);
    if (stderr.length > 0) console.log('stderr: ' + stderr);
    done(error);
  });


});






app.post('/api/get_heat_map', function(req, res) {


var data_body = req.body;

console.log(data_body);

var params_array = ['algos','caseids','metric','input','output','result_exe_id']

var cmd_params ='';



for(var key of params_array)
{

    // console.log("key: "+key)
    // console.log('value: '+value);

    var  value = data_body[key];
    var tmp = ' --'+key+' '+value;

    cmd_params = cmd_params+tmp;
}

console.log(cmd_params);




var unique_title = uuid.v1();


 queue.create('heat_map', {
    title: 'heat_map'+'_'+unique_title
  , cmd_params:  cmd_params
  
}).save();




res.send(unique_title);

});



var server = app.listen(8093, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

})








// ssssssssssssssssssss




