require('ext');
var fs = require('fs'),
		util = require('util');
var express = require('express'),
		chain_gang = require('chaingang'),
		uglify = require('uglify-js');


function clone(obj){
	if(obj == null || typeof(obj) != 'object') return obj;
	var temp = new obj.constructor(); // changed (twice)
	for(var key in obj) {
		temp[key] = clone(obj[key]);
	}
	return temp;
}
Object.has_key = function(obj, property) {
	return typeof(obj[property]) !== 'undefined'
};


/*
Files
=====

/config
	- JSON document containing configuration.

/:collection/stat
	- Statistics and other information about the collection.
	
	FORMAT {
		documents: ['john', 'jane']
	}

/:collection/document.stat
	- Stores versioning information and other statistics for the document.
	* This is cached in memory for every document!
	
	FORMAT {
		latest: "11223344", // Most up-to-date full document
		transformations: ["22334455", "33445566"] // List of transformations after the latest
	}

/:collection/document.:time
	- Stores a copy of either the full document or just a document with changed fields.

API
===

GET /_stats
	- Returns status of the server as plaintext.

GET /_collections
	- Returns a list of collections and their respective sizes.

GET /_collections/:name
	- Returns statistics for a collection.

GET /:collection
	- Returns a list of document ID's in the collection.

GET /:collection/:document
	- Returns the contents of a document within a collection.

POST /:collection/:document
	* Use with caution!
	- Update an entire document.

POST /:collection/:document/transform
	- Create a transformation on the document.

POST /:collection/:document/delete
	- Delete the document.

*/

var Venice = {
	collection: {document: {}}
};

Venice.new = function(path) {
	var venice = this;
	
	// DEFAULTS
	
	// Express server
	this.server = express.createServer();
	// Configuration
	this.config = {
		path: path,
		server: {
			port: 6475,
			host: 'localhost'
		},
		collections: []
	}
	var server = this.server, config = this.config;
	
	this.collections = {};
	
	
	
	// INITIALIZATION
	
	// Determine the environment to operate in. (Currently either production or development.)
	if(process.env.NODE_ENV === 'production') {
		this.env = 'production';
	} else {this.env = 'development';}
	this.server.set('env', this.env);
	
	
	
	// Reads the configuration for the server.
	this.configure = function(complete) {
		//c = Venice.collection.new(this, 'users');
		this.config = Object.merge(this.config, JSON.parse(fs.readFileSync(path + '/config')))
		
		if(this.development()) {
			//console.log('Configuration:');
			//console.log(this.config);
			console.log('server: Configuration loaded');
		}
		
		var total_collections  = this.config.collections.length;
		var loaded_collections = 0;
		this.config.collections.each(function(name) {
			venice.collections[name] = new Venice.collection.new(name, venice, function() {
				loaded_collections += 1;
				if(venice.development()) console.log('collections/'+name+': Collection loaded ('+loaded_collections+'/'+total_collections+')');
				
				if(loaded_collections === total_collections) {
					console.log('server: Collections loaded');
					complete();
				}
			});
		});
	}
	this.serve = function() {
		this.route();
	}
	this.development = function() {
		if(this.env === 'development') return true;
	}
	
	// ROUTING LAYER (Called by venice.serve())
	this.route = function() {
		server.get('/', function(req, res) {
			console.log('GET /');
			res.send('Hello.');
		});
		server.get('/:collection/:document', function(req, res) {
			if(Object.has_key(venice.collections.has_key, req.params.collection)) {
				var collection = venice.collections[req.params.collection];
				if(Object.has_key(collection.documents, req.params.document)) {
					var document = collection.documents[req.params.document];
					
					document.read(function(data, version) {
						res.header('X-Version', version);
						res.send(JSON.stringify(data));
					});
					return;
				} else {
					res.send('Document not found', 404);
				}
			} else {
				res.send('Collection not found', 404);
			}
			res.end();
		});
		server.get('/:collection/:document/transform', function(req, res) {
			if(Object.has_key(venice.collections, req.params.collection)) {
				var collection = venice.collections[req.params.collection];
				if(Object.has_key(collection.documents, req.params.document)) {
					var document = collection.documents[req.params.document];
					
					document.transform(function(doc) {
						doc.name = 'John';
					}, function() {
						res.send('Success!');
					}, function() {});
					return;
				} else {
					res.send('Document not found', 404);
				}
			} else {
				res.send('Collection not found', 404);
			}
			res.end();
		});
		
		//server.get('/collections')
		
		server.listen(config.server.port, config.server.host);
		console.log('server: Listening on '+config.server.host+':'+config.server.port)
	}
	
	// Finally, begin the configuration sequence to load the config and start the server.
	this.configure(function() {
		venice.serve();
	});
}

Venice.collection.new = function(name, venice_instance, collection_loaded_callback) {
	var collection = this;var venice = venice_instance;
	
	this.venice = venice;
	this.documents = {};
	this.stat = {
		'documents': [],
		'compaction_interval': 1 // Time in seconds between checking for compactions (should be fairly high for performance)
	}
	this.compaction_interval_id = null;
	
	this.name = name;
	this.path = venice.config.path+'/'+name;
	this.stat_queue = chain_gang.create({workers: 1}); // Limit to one worker so that jobs will execute sequentially
	
	this.add_document = function(name) {
		//collection.stat
	}
	this.stat_load = function(callback) {
		fs.readFile(this.path+'/stat', function(err, data) {
			if(err) throw err;

			collection.stat = Object.merge(collection.stat, JSON.parse(data));
			
			callback();
		});
	}
	
	this.compact = function() {
		
	}
	this.docs = {
		load: function(callback) {
			var total_docs = collection.stat.documents.length;
			var loaded_docs = 0;
			
			collection.stat.documents.each(function(name) {
				collection.documents[name] = new Venice.collection.document.new(name, collection, venice, function(document) {
					loaded_docs += 1;
					if(venice.development()) console.log('collections/'+collection.name+'/'+name+': Document loaded ('+loaded_docs+'/'+total_docs+')');
					
					
					if(loaded_docs === total_docs) {callback();}
				});
			});
		}
	}
	
	this.compact = function() {
		//console.log('Compacting if necessary.');
		Object.values(collection.documents).each(function(doc) {
			doc.compact();
		});
	}
	
	this.load = function(callback) {
		this.stat_load(function() {
			if(venice.development()) console.log('collections/'+collection.name+': Statistics loaded');
			
			collection.docs.load(function() {
				callback();
				
				collection.compaction_interval_id = setInterval(function() {
					collection.compact();
				}, collection.stat.compaction_interval * 1000);
				
			});
		});
	}
	
	this.load(collection_loaded_callback);
}



// Creates a template for a document.
Venice.collection.document.create = function(path) {}

// Instantiates a new managing class for a document.
Venice.collection.document.new = function(name, collection_instance, venice_instance, initialization_callback) {
	var document = this, collection = collection_instance, venice = venice_instance;
	
	this.collection = collection_instance;
	
	this.name = name;
	this.path = collection.path+'/'+this.name;
	///* DEPRECATED*/ this.stat_queue = chain_gang.create({workers: 1});
	
	// Used by save() and compact()
	this.version_write_chain = chain_gang.create({workers: 1});
	// Transformations
	this.transformation_queue	= []; // List of transformation versions to be compacted at the next interval
	this.transformation_cache	= {}; // Stores pending transformation functions by version.
	this.version = '';
	
	this.cache = {latest: null, latest_version: '0'};
	
	// Loads the statistics
	this.stat_load = function(callback) {
		fs.readFile(this.path+'.stat', function(err, data) {
			if(err) throw err;

			document.stat = JSON.parse(data);
			
			callback();
		});
	}
	
	// Update the statistics to have the latest version.
	this.stat_update_latest = function(callback) {
		//this.stat_queue.add(function(worker) {
			s = clone(document.stat);
			s.latest = document.version;
			s.transformations = document.transformation_queue;
			
			fs.writeFile(document.path+'.stat', JSON.stringify(s), 'utf8', function(err) {
				if(err) throw err;
				
				document.stat = s;
				//worker.finish();
				callback();
			});
		//});
	}
	
	// Reads and JSON.parse's a specific document version.
	this.read_version = function(version, callback) {
		fs.readFile(document.path+'.'+version, function(err, data) {
			if(err) throw err;
			
			callback(JSON.parse(data));
		});
	}
	
	// If passed a function, it will give you the latest version in either of two ways:
	// 		1. Either serving from an up-to-date cache
	// 		2. Reading the latest full version from file.
	// If give an object of format {data: '...', version: '123'}, it will update the cache with that object.
	this.update_latest_cache = function(v) {
		if(typeof v === 'function') {
			if(parseInt(document.cache.latest_version) < parseInt(document.stat.latest)) {
				document.read_version(document.stat.latest, function(data) {
					document.cache.latest = data;
					document.cache.latest_version = document.stat.latest;
					document.version = document.stat.latest;
					v(data, document.cache.latest_version);
				});
			} else {
				v(document.cache.latest, document.cache.latest_version);
			}
		} else {
			this.cache.latest = v.data;
			if(v.version) this.cache.latest_version = v.version;
		}
	}
	// Grabs the original latest version, runs all queued transformations on it, then saves the new version as the latest.
	this.compact = function(callback) {
		//console.log('Compacting!');
		
		if(document.transformation_queue.length > 0) {
			document.version_write_chain.add(function(worker) {
				
				document.read_version(document.stat.latest, function(data) {
					var state = clone(data), version = document.stat.latest;
					
					//if(venice.development()) console.log(document.console_name()+'.'+transformation.version+': Transformation saved');
					var t_start = (new Date()).getTime();
					if(venice.development()) console.log(document.console_name()+'/compaction: Beginning compaction');
					
					var unlink_list = [];
					
					// Variables for each transformation
					var t_ver = null, t_state = false, t_func = null;
					
					// Iterating through all available queued transformations
					while(t_ver = document.transformation_queue.shift()) {
						t_func = document.transformation_cache[t_ver];
						
						t_state = document.transform_state(state, t_func);
						if(t_state !== false) {
							state = t_state;
						}
						previous_version = t_ver;
						delete document.transformation_cache[t_ver]; // Delete the cached function
						unlink_list.push(t_ver); // Add the transformation version to the list to be eventually unlinked
						
						if(venice.development()) console.log(document.console_name()+'/compaction: Applied transformation '+t_ver);
					}
					
					if(venice.development()) console.log(document.console_name()+'/compaction: Transformations complete; saving');
					
					new_version = previous_version;
					
					fs.writeFile(document.path+'.'+new_version, JSON.stringify(state), function(err) {
						if(err) throw err;
						
						document.version = new_version;
						document.stat_update_latest(function() {
							document.update_latest_cache({
								'data': state,
								'version': new_version
							});
							
							if(venice.development()) console.log(document.console_name()+'/compaction: Compaction complete in '+((new Date()).getTime() - t_start)+'ms; new version is '+new_version);
							
							// Go through and eventually unlink each transformation version
							unlink_list.each(function(ver) {
								fs.unlink(document.path+'.'+ver+'.transformation')
							});
							
							worker.finish();
							
						}); // stat_update_latest()
						
					}); // fs()
					
				});
				
			}); // version_write_chain.add()
			
		} else {} // No transformations in the queue
	}
	this.transform = function(tfunc, cache_callback, callback) {
		//document.transformation_queue.add
		//var state = document.transform_state(clone(document.cache.latest));
		var transformation = {
			function: tfunc,
			version: (new Date()).getTime()+''
		}
		
		var state = document.transform_state(document.cache.latest, transformation.function);
		
		if(state === false) {
			callback(true);
		} else {
			document.update_latest_cache({
				'data': state//,
				//'version': transformation.version
			});

			cache_callback(); // Fire the callback now that the cache has been updated.
			
			var func_string = document.util.compress('('+transformation.function.toString()+')');
			
			fs.writeFile(document.path+'.'+transformation.version+'.transformation', func_string, function(err) {
				if(err) throw err;
				
				// Save the document to the queue and cache the compressed function.
				document.transformation_queue.push(transformation.version);
				document.transformation_cache[transformation.version] = transformation.function;
				
				//console.log(document.cache);
				document.stat_update_latest(function() {
					if(venice.development()) console.log(document.console_name()+'.'+transformation.version+': Transformation saved');
					
					callback();
				});
				
			}); // fs.writeFile()
			
		} // transform_state check
	}
	this.util = {
		compress: function(s) {
			var ast = uglify.parser.parse(s); // Parse the code to build an AST
			ast = uglify.uglify.ast_mangle(ast); // Mangle (shorten) the names
			ast = uglify.uglify.ast_squeeze(ast); // Compress
			return uglify.uglify.gen_code(ast);
		}
	}
	
	// Takes a state and transformation func and runs the transformation on the state (document's data).
	this.transform_state = function(state, tfunc) {
		var state = clone(state); // For good measure
		ret = tfunc(state);
		
		if(ret === false) {
			return false;
		} else {
			return state
		}
	}
	
	// Continues the loading process to finish up stuff that couldn't be done at the beginning of the object definition.
	// Loads the document statistics, then loads the latest cache.
	this.load = function(callback) {
		this.stat_load(function() {
			document.update_latest_cache(function(data, version) {
				callback();
			});
		});
	}
	
	/* 	Saves a full copy of the document to the database and sets that copy as the latest version.
			
			EXECUTION FLOW:
					1. Check type of input data (string or not?)
					2. Add the following instructions to the version queue.
							1. Calculate a new version time
							2. Write the document
									1. Update the statistics file for the latest version
											1. Update the cache for the latest version
											2. Call the given callback
											3. Tell the version queue that the job is done ("worker.finish()")
	*/
	this.save = function(input_data, cache_callback, complete_callback) {
		if(typeof input_data !== 'string') {
			var data = JSON.stringify(input_data);
		} else {
			var data = input_data;
		}
		// Compaction needs to run before a new full version can be saved
		document.compact(function() {
			
			document.version_write_chain.add(function(worker) {
				var version = (new Date()).getTime()+'';

				// Actually go about writing the version.
				fs.writeFile(document.path+'.'+version, data, function(err) {
					if(err) throw err;

					// Update the stat file for the document
					document.version = version;
					document.stat_update_latest(function() {
						// Now that everything is done, update the document cache with the latest version.
						document.update_latest_cache({
							'data': input_data,
							'version': version
						});

						if(venice.development()) console.log(document.console_name()+'.'+version+': Version saved');

						complete_callback(); // Log it to the console

						worker.finish();

					}); // stat_update_latest()
				}); // fs()
			}); // version_write_chain.add()
			
			cache_callback(); // Fire off a callback to recognize that it's been added to the queue successfully.
			
		}); // compact()
		
	}
	// Saves a copy of a version.
	this.backup = function(version) {
		var is = fs.createReadStream(document.path+'.'+version)
		var os = fs.createWriteStream(document.path+'.'+version+'.backup');

		util.pump(is, os, function(err) {
			if(err) throw err;
			//fs.unlinkSync('source_file');
		});
		
	}
	this.read = function(callback) {
		document.update_latest_cache(function(data, version) {
			callback(data, version)
		});
	}
	this.console_name = function() {return 'collections/'+collection.name+'/'+document.name;}
	
	
	this.load(function() {initialization_callback(document);});
}

d = new Venice.new('/projects/madelike/venice/test2');