/**
 *  DashboardServer to bridge the communication between Dashboards and database.
 *
 *  @version 3.0.0
 */

'use strict';

var
    // Load local modules
    config = require('./config'),
    lca = require('./lca'),
    ReportObject = require('./report'),
    NilACLServerConnection = require('./nilacl'),
    Cache = require('node-cache'),

    // Load Node.js modules
    http = require('http'),
    pg = require('pg'),
    url = require('url'),
    async = require('async'),
    cache = new Cache(),

    // Set up script variables
    dbClient,
    dbConnectionString = ('postgres://' +
                            config.databases.olap.username + ':' +
                            config.databases.olap.password + '@' +
                            config.databases.olap.host + ':' +
                            config.databases.olap.port + '/' +
                            config.databases.olap.database),

    // Misc variables
    i, len;



/**
 *  Execute query for the module on the dbClient using specified params.
 *
 *  @param  {object}    dbClient    Database client taken from connection pool
 *  @param  {object[]}  params      The parameters to be passed to the module
 *  @param  {string}    module      The name of the module to be processed
 *  @param  {function}  callback    The callback function after performing query
 *
 */

var performQuery = function (req, dbClient, params, cache, moduleName, callback) {
    var ModuleObject = require('./' + moduleName.toLowerCase()),
        moduleObjectPrototype = ModuleObject.prototype,
        method,
        module;

    ModuleObject.prototype = new ReportObject();
    ModuleObject.prototype.constructor = ModuleObject;

    for (method in moduleObjectPrototype) {
        if (moduleObjectPrototype.hasOwnProperty(method)) {
            ModuleObject.prototype[method] = moduleObjectPrototype[method];
        }
    }

    module = new ModuleObject(lca.stringToUpperCamelCase(moduleName), params, dbClient, cache, false);

    req.nilacl.sitePrincipalQueryParameter(req.oauth_token, moduleName, function (sitePrincipal) {
        params.combo = sitePrincipal;
        if (!!module.runExternalQuery) {
            module.runExternalQuery(function (error, query) {
                if (error) {
                    module.errors = error;
                } else {
                    module.sql = query;
                }
                module.execute(callback);
            });
        } else {
            module.execute(callback);
        }
    });

};

var verifyOauth = function (req) {
    req.nilacl = new NilACLServerConnection();

    try {
        req.oauth_token = lca.extractOAuthAuthenticationVars(req.headers).oauth_token;
    } catch (e) {
        throw e;
    }
};

/**
 *  Handle errors that occured during execution
 *
 *  Note: Don't know how to implement this yet. Copied from the node-pg example.
 *
 *  @param  {object}    error   The object which contains error information
 *
 */
var handleError = function (res, err, dbClient, done) {
    /*
     * An error occurred, remove the client from the connection pool.
     * A truthy value passed to done will remove the connection from the pool
     * instead of simply returning it to be reused.
     * In this case, if we have successfully received a client (truthy)
     * then it will be removed from the pool.
     */

    res.writeHead(400, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(err));

    if (!!dbClient) {
        done(dbClient);
    }
};


/**
 *  Server module to handle incoming requests from Dashboards
 *
 *  @param  {event}     Function to handle http server request event.
 *
 */
var dashboardServer = http.createServer(function (req, res) {
    var
        jsonResult = [],
        urlParts = url.parse(req.url, true),
        requests,
        params,
        returnType;
    try {
        requests = JSON.parse(urlParts.query['query_parameters[0]']);
        params = JSON.parse(urlParts.query['query_parameters[1]']);
        returnType = urlParts.query.return_type;
    } catch (e) {
        handleError(res, {  'code'      : 1,
                            'message'   : 'Invalid or missing parameters'
                         });
    }

    try {
        verifyOauth(req);
    } catch (e) {
        throw e;
    }

    // Is wrong should implement routing not implemented yet
    if (!!requests) {
        // Establish connection to the database
        pg.connect(dbConnectionString, function (err, dbClient, done) {
            if (err) {
                handleError(res, err, dbClient, done);
            }

            // Map the requests coming in from dashboards and performQuery
            // based on the module and the parameters passed in
            async.map(requests,
                        performQuery.bind(null, req, dbClient, params, cache),
                        function (err, result) {
                    if (err) {
                        handleError(res, err, dbClient, done);
                    } else {
                        if (!!returnType) {
                            result.forEach(function (i) {
                                var results = [];
                                i.rows.forEach(function (row, index) {
                                    var strings = '';
                                    if (returnType === 'arrayWithHeader' &&
                                            index === 0) {
                                        strings =
                                            Object.keys(row).join('\t') + '\n';
                                    }

                                    Object.keys(row).forEach(function (key, index) {
                                        strings += row[key];
                                        if (index < Object.keys(row).length - 1) {
                                            strings += '\t';
                                        }
                                    });
                                    results.push(strings);
                                });
                                jsonResult.push(results.join('\n'));
                            });
                        } else {
                            for (i = 0, len = result.length; i < len; i += 1) {
                                jsonResult.push(result[i].rows);
                            }
                        }


                        // Send response back to dashboard
                        res.writeHead(200, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify(jsonResult));
                        done(dbClient);
                    }
                });
        });
    }


});

// Start and wait for connection
dashboardServer.listen(config.default.dashboardServerPort);
