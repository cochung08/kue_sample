
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require("fs");

var uuid = require('node-uuid');

var exec = require('child_process').exec;




app.use(bodyParser.json()); 
app.use(bodyParser.urlencoded({ extended: true }));




var kue = require('kue');
var queue = kue.createQueue({
    disableSearch: false
});






var fs = require('fs');






queue.on( 'error', function( err ) {
  console.log( 'Oops... ', err );
   console.log( 'Oops... ', err );
    console.log( 'Oops... ', err );
     console.log( 'Oops... ', err );
      console.log( 'Oops... ', err );
});




queue.process('heat_map',2, function(job, done){



var spark_path = "/home/cochung/spark_full" ;
  var command1 = '/home/cochung/spark_full/spark-1.4.1/bin/spark-submit --class sparkgis.SparkGISMain /home/cochung/spark_full/spark-gis/target/uber-spark-gis-1.0.jar';
  

  var job_title = job.data.title
  var cmd_params = job.data.cmd_params

  var job_file =job.id+".txt"
  var log_cmd = " > "+job_file


   exec(command1+ cmd_params + log_cmd,function(error, stdout, stderr){

  console.log('stdout: ' + stdout);
  console.log('!!!!!!!!!!!!!!: ');
  console.log('stderr: ' + stderr);
 //   var err = new Error( stderr);
 
 // console.log("done error");
 // console.log(  err   );

var output = fs.readFileSync(job_file).toString().split("\n");

var len = output.length


var status = output[len-2];



      if (status =='completed')
    {
      console.log(  status        );
      done()

      // console.log('stderr: ' + stderr);
     
     
      } 
      else
     {
      var err = new Error( stderr);
      done(err);
       console.log("done error");

     }





   




// if (error) {
//    console.error("error:   "+ error);
//     console.error(`exec error: ${error}`);
//     done(error);
//   }

//   else
//   {
//     console.log("no error");
//     done();
//   }
  









    
  });



 // var domain = require('domain').create();
 //  domain.on('error', function(err){
 //    done(err);
 //  });
 //  domain.run(function(){ 

 //    exec(command1+ cmd_params+log_cmd,function(error, stdout, stderr){
 //    if (stdout.length > 0) console.log('stdout: ' + stdout);
 //    if (stderr.length > 0) console.log('stderr: ' + stderr);
 //    done();
 //  });
 //  });




});






app.post('/api/get_heat_map', function(req, res) {


var data_body = req.body;

console.log(data_body);

var params_array = ['algos','caseids','metric','input','output','result_exe_id']


// var params_array = ['caseids','metric','input','output','result_exe_id']
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

var job_id;

 var job = queue.create('heat_map', {
    title: unique_title
  , cmd_params:  cmd_params
  
}).save( function(){
    job_id = job.id ;
    console.log("unique_job_id: "+job_id)
    res.send('job_id: '+job_id);
});






});



var server = app.listen(8099, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

})








// ssssssssssssssssssss




