
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








var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');



var url = 'mongodb://localhost:27017/u24_segmentation';
MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("MOngo Connected correctly to server.");
  db.close();
});





var find_heat_map_by_jobid = function(db, job_id,callback) {



 



}





app.post('/api/get_heat_map_result', function(req, res) {


var data_body = req.body;

console.log(data_body);

var key = 'jobid'


    var  job_id = data_body[key];
console.log(job_id);




MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);




    var cursor =db.collection('ad_hoc_results').find(  {   "job_id":  job_id }      );

   cursor.toArray(function (err, results) {

   	 db.close();


   	 var result_json = {}


   	 for(var i=0;i< results.length; i++)
   	 {
   	 	result_json[i] =   results[i]
   	 }

	    if (!err) {
	    	console.log(result_json)
	    	res.send('results: '+JSON.stringify (results));
    
    }
});


   



});





});







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

  
  });






});






app.post('/api/get_heat_map', function(req, res) {


var data_body = req.body;

console.log(data_body);

var params_array = ['uid','algos','caseids','metric','input','output','result_exe_id']


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






    res.send('job_id: '+unique_title);
});






var insert_status_with_jobid = function(status,jobid)
{

	MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);



 db.collection('ad_hoc_results').insertOne( {
      "jobid" : jobid,
      "status" :status
      
    
   }, function(err, result) {
    assert.equal(err, null);
    console.log("Inserted a document into the restaurants collection.");
      db.close();
  });






});

}









var update_status_by_jobid = function(status,jobid)
{

	MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);



db.collection('ad_hoc_results').updateOne(
      {"jobid" : jobid },
      {
        $set: {  "status" :status },
        $currentDate: { "lastModified": true }
      }, function(err, results) {
      console.log("updated");
      db.close();
   });




});

}







var updateStatusByJobId = function(db,status,jobid, callback) {
   db.collection('ad_hoc_results').updateOne(
      {"jobid" : jobid },
      {
        $set: {  "status" :status },
        $currentDate: { "lastModified": true }
      }, function(err, results) {
      console.log(results);
      callback();
   });
};









job.on('complete', function(result){
  // console.log('Job completed with data ', result);

update_status_by_jobid('complete',unique_title)




}).on('failed attempt', function(errorMessage, doneAttempts){
  console.log('Job failed');

}).on('start', function(errorMessage, doneAttempts){
  console.log('Job started');



update_status_by_jobid('start',unique_title)



}).on('enqueue', function(result){

	console.log('enqueue12');

insert_status_with_jobid('enqueue',unique_title)
// update_status_by_jobid('enqueue',unique_title)

  

}).on('progress', function(progress, data){
  // console.log('\r  job #' + job.id + ' ' + progress + '% complete with data ', data );
console.log("ssssssssss")
});








});



var server = app.listen(8399, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

})











// ssssssssssssssssssss




var insertStatusByJobId = function(db,status, jobid,callback) {
   db.collection('ad_hoc_results').insertOne( {
      "jobid" : jobid,
      "status" :status
      
    
   }, function(err, result) {
    assert.equal(err, null);
    console.log("Inserted a document into the restaurants collection.");
    callback();
  });




}




var updateStatusByJobId = function(db,status,jobid, callback) {
   db.collection('ad_hoc_results').updateOne(
      {"jobid" : jobid },
      {
        $set: {  "status" :status },
        $currentDate: { "lastModified": true }
      }, function(err, results) {
      console.log(results);
      callback();
   });
};