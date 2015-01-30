'use strict';
var config = require('./config'),
    log4js = require('log4js'),
    crypto = require('crypto'),
    logger = log4js.getLogger(config.log4js.logCategory);

log4js.configure({ appenders: config.log4js.appenders });
logger.setLevel(config.log4js.debugLevel);

function Report() {
    /*  Base class for reports.
        Encapsulates report parameters and execution logic. Subclasses may override
        report parameters and must provide an _sql method which returns SQL that is
        used by the execute method to generate results for the report.
    */

    this.expiry = 3600;
    this.parameter_regex = new RegExp(/%(\w+)%/g);
}

Report.prototype = {
    execute: function (error, callback) {
        //handles any exception thrown by modules
        if (!!this.errors) {
            return error(this.errors);
        }

        /*
            Executes the query and returns encoded results.
            Results are encoded based on the return_type request parameter.
        */
        try {
            this.prepareQuery(error, callback);
        } catch (e) {
            return error(e);
        }
        this.runQuery(this.final_sql, this.final_parameters, this.cache, error, callback);
    },

    getMappedArray : function () {
        if (!!this.parameters.map) {
            this.parameter.map.forEach(function (map) {
                if (this.sequence === map.sid) {
                    return map;
                }
            });
        }
    },

    mergeParameter : function (mapped_key) {
        var postfix = '_' + String(mapped_key);
        this.parameters.forEach(function (key) {
            if (key.search(postfix) > 0) {
                this.parameters[key.replace(postfix, '')] =
                    this.parameters[key];
            }
        });
    },

    convertReportsParameters : function () {
        /*
            This is to get specific key based on query sequence,
            for support different parameters on the same query reports cases:
                1. Dual date range support
                2. Call twice on same query
            It will convert the map:[{'sid': x, 'key1': '', 'key2': ''}]. x
            will be sequence of the reports. key1/2 is supporting cases.
            If map 'sid' found, it will start merging the parameters based
            on key1/2 postfix
        */
        var map = this.getMappedArray();
        if (!map) {
            return;
        }
        if (!!map.key1) {
            this.mergeParameter(map.key1);
        }
        if (!!map.key2) {
            this.mergeParameter(map.key2);
        }
        if (!!map.key3) {
            this.mergeParameter(map.key3);
        }
    },

    checkRequiredParameters : function () {
        var parameters = this.parameters,
            missing = [];

        this.required_parameter.forEach(function (key) {
            if (!parameters[key]) {
                missing.push(key);
            }
        });
        if (missing.length > 0) {
            logger.error('Invalid parameters, missing key!');
            throw ({
                'error-messsage': 'Invalid parameters, missing key! : [' +
                    missing.join(', ') + ']',
                'report_name': this.name
            });
        }

    },

    prepareQuery : function () {
        /*
            Prepare the sql query and parameters for later execute
        */
        if (this.cache_enabled) {
            logger.debug('Cache miss for ' + this.name);
        }

        try {
            this.checkRequiredParameters();
        } catch (e) {
            throw e;
        }

        var parameters = this.parameters,
            report_parameters = this.sql.match(this.parameter_regex),
            parameters_length =
                report_parameters ? report_parameters.length : 0,
            final_parameters = [],
            i = 0;

        if (!!report_parameters) {
            report_parameters = report_parameters.map(function (x) {
                return x.replace(/\%/g, '');
            });

            report_parameters.forEach(function (p) {
                final_parameters.push(parameters[p]);
            });
        }
        this.final_parameters = final_parameters;
        this.final_sql = this.sql;
        for (i = 1; i < parameters_length + 1; i += 1) {
            this.final_sql = this.final_sql.replace(/%\w+%/, '$' + i);
        }

    },

    runQuery : function (query, parameters, cache, callback) {
        var module = this,
            start = new Date(),
            cacheKey = crypto.createHash('sha1').update(query + parameters.toString()).digest('hex');


        if (Object.keys(cache.get(cacheKey)).length > 0) {
            callback(null, cache.get(cacheKey)[cacheKey]);
            return;
        }

        logger.debug('Executing query : ' + module.name);
        this.db.query(query, parameters, function (err, result) {
            if (err) {
                callback(err);
            } else {
                cache.set(cacheKey, result);
                callback(null, result);
                logger.debug('Finished executing query : ' + module.name);
                logger.debug(module.name +
                                ' took ' +
                                (new Date() - start) +
                                ' miliseconds to complete.');
            }
        });

    }

};

module.exports = Report;
