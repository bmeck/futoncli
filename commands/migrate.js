var flatiron = require('flatiron');
var futoncli = require('../futoncli');
var helpers = require('../helpers');
var path = require('path');
var migrate = exports;

migrate.test = function (callback) {
  var server = futoncli.server;
  
  var optionsSchema = [
    // properties: {
       //backupServer:
       //{
       //  pattern: /https?:[\/]+[^\/]+/i,
       //  message: 'Backup Server must be a url without the database name',
       //  description: 'Server to create backup on',
       //  required: false
       //},
       ////backupDatabase:
       //{
       //  name: 'S1',
       //  description: 'Database to backup to',
       //  required: false
       //},
       //testServer:
       {
         name: 'testServer',
         message: 'Test Server',
         description: 'Server to test migration on',
         pattern: /^https?:[\/]+[^\/]+[\/]$/i,
         required: true
       },
       //testDatabase:
       {
         name: 'testDatabase',
         message: 'Test Database',
         description: 'Database to test migration on',
         required: false
       },
       //migrationScript:
       {
         name: 'migrationScript',
         message: 'Migration Script',
         description: 'File of migration plan (must include a test(doc)=>bool and migrate(doc)=>doc function)',
         required: true
       }
    // }
  ]
  futoncli.prompt.get(optionsSchema, function (err, results) {
    if(err) {
      callback(err);
      return;
    }
    var plan = require(path.resolve(results.migrationScript));
    
    var missing = [];
    if (!plan.migrate) {
      missing.push('migrate');
    }
    if (!plan.test) {
      missing.push('test');
    }
    if (missing.length) {
      callback('Migration plan is missing functions for: ' + missing);
      return;
    }
    
    var testServer = require('nano')(results.testServer);
    var backupServer;
    if (results.backupServer) backupServer = require('nano')(results.backupServer);
    
    var backupDatabase = results.backupDatabase || 'MigrationBackup-' + Date.now();
    var testDatabase = results.testDatabase || 'MigrationTesting-' + Date.now();
    
    var replicator = require('nano')(server.config.url);
    function createBackup(next) {
      if (!backupServer) {
         next();
         return;
      }
      backupServer.db.create(backupDatabase, function (err) {
         if (err) {
            next(err);
            return;
         }
         var source = futoncli.config.get('endpoint');
         var target = backupServer.config.url + backupDatabase;
         replicator.db.replicate(source, target, {}, next);
      });
    }
    function createTesting(next) {
      testServer.db.create(testDatabase, function (err) {
         if (err) {
            next(err);
            return;
         }
         var source = futoncli.config.get('endpoint');
         var target = testServer.config.url + testDatabase;
         replicator.db.replicate(source, target, {}, next);
      });
    }
    function performTestMigration(next) {
      testDatabase = testServer.use(testDatabase);
      testDatabase.list({include_docs: true}, function (err, results) {
         if (err) {
            next(err);
            return;
         }
         var docs = results.rows || [];
         docs = docs.map(function (row) {
            return plan.migrate(row.doc);
         });
         testDatabase.bulk({
            docs: docs
         }, {}, next);
      });
    }
    function performTestConfirmation(next) {
      testDatabase.list({include_docs: true}, function (err, results) {
         if (err) {
            next(err);
            return;
         }
         var docs = results.rows || [];
         var bad = docs.filter(function (row) {
            return !plan.test(row.doc);
         }).map(function (row) {
            return row.doc;
         });
         if (bad.length) {
            var problem = new Error('Problem with migration on docs for : ');
            problem.docs = bad;
            next(problem);
         }
         else {
            next();
         }
      });
    }
    
    flatiron.common.async.series([
      createBackup,
      createTesting,
      performTestMigration,
      performTestConfirmation
    ], function (err) {
      if (err) {
         helpers.generic_cb(callback)(err, err.docs || {})
      }
      else {
         helpers.generic_cb(callback)(null, {});
      }
    });
    
  });

};

migrate.perform = function (name, callback) {
  var server = futoncli.server;

  if(typeof name === "function") {
    name(new Error("You didn't provide a database name."));
    return;
  }

  server.db.get(name, helpers.generic_cb(callback));
};

migrate.usage = [
  '',
  '`futon migrate *` commands allow you to edit your',
  'local futon configuration file. Valid commands are:',
  '',
  'futon migrate perform',
  'futon migrate test'
];