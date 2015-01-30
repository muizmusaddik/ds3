'use strict';
var config = require('../reports/config'),
    pg = require('pg'),
    fs = require('fs'),
    moment = require('moment'),
    opt = process.argv.slice(2),
    program = require('commander');

function LogParsing() {
    this.ds3Names = ['date', 'time', 'level', 'processid', 'threadid', 'component', 'user_id', 'log'];
    this.ds3LineLogpats = new RegExp(/(\S+) (\S+) (\S+)\s*(\S+) (\S+) (\S+) (\S+) ([\w\W]*)/);
}

LogParsing.prototype = {
    getUsernameList : function (callback) {
        var dbConnectionString = ('postgres://' +
                            config.databases.nilacl.username + ':' +
                            config.databases.nilacl.password + '@' +
                            config.databases.nilacl.host + ':' +
                            config.databases.nilacl.port + '/' +
                            config.databases.nilacl.database),
            client = new pg.Client(dbConnectionString);

        client.connect(function (err) {
            if (err) {
                return console.error('could not connect to postgres', err);
            }
            client.query('SELECT id, username FROM users;', function (err, result) {
                if (err) {
                    return console.error('error running query', err);
                }
                callback(result);
                client.end();
            });
        });
    },

    parseDs3LogLine: function (line) {
        var match = line.match(this.ds3LineLogpats),
            result = {},
            i;
        if (!match) {
            console.log(line);
            throw 'Matches nothing: null';
        }
        for (i = 0; i < this.ds3Names.length; i += 1) {
            result[this.ds3Names[i]] = match[i + 1];
        }
        return result;
    },

    dt: function (r) {
        // example: 2011-07-07 08:40:26, 312
        var dateString = r.date + ' ' + r.time.replace(',', '.');
        return moment(dateString);
    },

    dtToString : function (dt) {
        // example: 07-07 08:40:26.312
        return dt.format('DD-MM h:mm:ss.SSS');
    },

    tabulateDurations: function (r, keysToTabulate) {
        // Given a dict with any number of columns * _start_time and *_end_time
        // pairs, calculate the difference of those values and store them in the dict
        // element * _duration
        var d;
        keysToTabulate.forEach(function (k) {
            d = moment.duration(r[k + '_end_time'] - r[k + '_start_time']);
            r[k + '_duration'] = d.milliseconds() + (d.seconds() + d.days() * 24 * 3600) * Math.pow(10, 6) / Math.pow(10, 6);
        });
    },

    getRequestQueries: function (reqLogLines) {
        var queryCount = 0,
            start,
            reqQueries = {},
            cacheKeyStart,
            logParts,
            that = this;

        reqLogLines.forEach(function (r) {
            if (!reqQueries[queryCount]) {
                reqQueries[queryCount] = {
                    'total_start_time': moment(0),
                    'total_end_time': moment(0),
                    'name': null,
                    'parameters': null,
                    'from_cache': 1,
                    'cache_key': null,
                    'nilacl_start_time': moment(0),
                    'nilacl_end_time': moment(0),
                    'query_start_time': moment(0),
                    'query_end_time': moment(0),
                    'encoding_start_time': moment(0),
                    'encoding_end_time': moment(0),
                    'nilacl_duration': 0,
                    'query_duration': 0,
                    'cache_duration': 0,
                    'encoding_duration': 0,
                    'missing_duration': 0,
                    'total_duration': 0
                };
            }
            if (r.log.search('Querying NilACL to check if user with OAuth token') !== -1) {
                start = that.dt(r);
                reqQueries[queryCount].total_start_time = start;
                reqQueries[queryCount].nilacl_start_time = start;
            }

            if (r.log.search('Time elapsed:') !== -1) {
                reqQueries[queryCount].nilacl_end_time = that.dt(r);
            }

            if (r.log.search('Opening') !== -1) {
                start = that.dt(r);
                logParts = r.log.split(' ');
                reqQueries[queryCount].name = logParts[1];
                reqQueries[queryCount].parameters = logParts[2].split(':')[1];
            }

            if (r.log.search('Cache lookup') !== -1) {
                cacheKeyStart = r.log.search('Cache lookup') + 'Cache lookup for '.length;
                reqQueries[queryCount].cache_start_time = that.dt(r);
                reqQueries[queryCount].cache_key = r.log.substring(cacheKeyStart);
            }

            if (r.log.search('Cache miss') !== -1) {
                reqQueries[queryCount].from_cache = 0;
                reqQueries[queryCount].cache_end_time = that.dt(r);
            }

            if (r.log.search('Executing query') !== -1) {
                reqQueries[queryCount].query_start_time = that.dt(r);
            }

            if (r.log.search('Finished executed query') !== -1) {
                reqQueries[queryCount].query_end_time = that.dt(r);
            }

            if (r.log.search('Encoding results') !== -1) {
                if (reqQueries[queryCount].from_cache) {
                    reqQueries[queryCount].cache_end_time = that.dt(r);
                }

                reqQueries[queryCount].encoding_start_time = that.dt(r);
            }

            if (r.log.search('Finished encoded results') !== -1) {
                reqQueries[queryCount].total_end_time = that.dt(r);
                reqQueries[queryCount].encoding_end_time = that.dt(r);

                that.tabulateDurations(
                    reqQueries[queryCount],
                    ['nilacl', 'total', 'query', 'cache', 'encoding']
                );

                reqQueries[queryCount].missing_duration =
                    reqQueries[queryCount].total_duration -
                    reqQueries[queryCount].nilacl_duration +
                    reqQueries[queryCount].query_duration +
                    reqQueries[queryCount].cache_duration +
                    reqQueries[queryCount].encoding_duration;

                queryCount += 1;
            }
        });
        return reqQueries;
    },

    processRequest: function (requestLogLines, requestQueries) {
        // request_log_lines - All the raw log lines associated with this request
        // request_queries   - Processed array storing information
        // pertaining to this requests sub queries.
        var r = {
            'user_id': null,
            'request_start_time': null,
            'request_end_time': null,
            'sub_request_count': null,
            'from_cache': null,
            'query_duration': null,
            'cache_duration': null,
            'encoding_duration': null,
            'total_duration': null,
            'request_duration': null,
            'missing_duration': null
        },
            keysToSum = ['nilacl_duration', 'query_duration', 'cache_duration', 'encoding_duration',
                         'total_duration', 'from_cache', 'missing_duration'];

        r.request_start_time = this.dt(requestLogLines[0]);
        r.request_end_time = this.dt(requestLogLines[requestLogLines.length - 1]);
        r.user_id = requestLogLines[0].user_id;
        r.sub_request_count = Object.keys(requestQueries).length;
        keysToSum.forEach(function (key) {
            r[key] = Object.keys(requestQueries).map(function (i) {
                return requestQueries[i][key];
            }).reduce(function (a, b) {
                return a + b;
            });
        });
        this.tabulateDurations(r, ['request']);
        return r;
    },

    ppRequest: function (request, title) {
        title = title || false;
        if (title) {
            console.log('Total         Start              C?/R Query    Cache    Miss     User');
        } else {
            if (!!request) {
                request.request_start_time = this.dtToString(request.request_start_time);
                request.request_count = request.from_cache + "/" + request.sub_request_count;
                console.log(
                    request.request_duration.toFixed(3) + this.whiteSpaces(13, request.request_duration.toFixed(3).length) + ' ' +
                        request.request_start_time + this.whiteSpaces(18, String(request.request_start_time).length) + ' ' +
                        request.request_count + this.whiteSpaces(8, String(request.request_count).length) + ' ' +
                        request.query_duration + this.whiteSpaces(8, String(request.query_duration).length) + ' ' +
                        request.cache_duration + this.whiteSpaces(8, String(request.cache_duration).length) + ' ' +
                        request.missing_duration + this.whiteSpaces(8, String(request.missing_duration).length) + ' ' +
                        request.user_id + this.whiteSpaces(5, String(request.user_id).length)
                );
            }
        }
    },
    ppRequestQueries: function (subRequestRows, processedRequests) {
        var r;
        console.log('Total         Start              C?       Query    Cache    Enc      Acl      Miss     IUD   Report ');
        for (r in  subRequestRows) {
            if (subRequestRows.hasOwnProperty(r)) {
                subRequestRows[r].total_start_time = this.dtToString(subRequestRows[r].total_start_time);
                subRequestRows[r].user_id = processedRequests.user_id;
                console.log(
                    subRequestRows[r].total_duration.toFixed(3) + this.whiteSpaces(13, String(subRequestRows[r].total_duration.toFixed(3)).length) + ' ' +
                        subRequestRows[r].total_start_time + this.whiteSpaces(18, String(subRequestRows[r].total_start_time).length) + ' ' +
                        subRequestRows[r].from_cache + this.whiteSpaces(8, String(subRequestRows[r].from_cache).length) + ' ' +
                        subRequestRows[r].query_duration + this.whiteSpaces(8, String(subRequestRows[r].query_duration).length) + ' ' +
                        subRequestRows[r].cache_duration + this.whiteSpaces(8, String(subRequestRows[r].cache_duration).length) + ' ' +
                        subRequestRows[r].encoding_duration + this.whiteSpaces(8, String(subRequestRows[r].encoding_duration).length) + ' ' +
                        subRequestRows[r].nilacl_duration + this.whiteSpaces(8, String(subRequestRows[r].nilacl_duration).length) + ' ' +
                        subRequestRows[r].missing_duration + this.whiteSpaces(8, String(subRequestRows[r].missing_duration).length) + ' ' +
                        subRequestRows[r].user_id + this.whiteSpaces(5, String(subRequestRows[r].user_id).length) + ' ' +
                        subRequestRows[r].name
                );
            }


        }
    },

    generateRequestTotalReport: function (processedRequests) {
        var users = [],
            uniqueUsers = [],
            totalTime = 0,
            totalCache = 0,
            totalQuery = 0,
            totalFromCache = 0,
            totalSubRequestCount = 0,
            totalRequest;
        processedRequests.forEach(function (d) {
            totalTime += d[0].request_duration;
            totalCache += d[0].cache_duration;
            totalFromCache += d[0].from_cache;
            totalSubRequestCount += d[0].sub_request_count;
            totalQuery += d[0].query_duration;
            users.push(d[0].user_id);
        });

        users.forEach(function (i) {
            if (!uniqueUsers.hasOwnProperty(users[i])) {
                uniqueUsers.push(users[i]);
            }
        });
        totalRequest = parseFloat(Object.keys(processedRequests).length);

        console.log('Request: ' + totalRequest + ' ' +
                    'Subrequest/Cached: ' + totalSubRequestCount + ', ' +
                    'Time: ' + totalFromCache + ', ' +
                    'Avg time/request: ' + Math.round(totalTime / totalRequest, 3));
        console.log('Cache time: ' + totalCache + ', ' + Math.round(totalCache / totalTime, 3) * 100);
        console.log('Query time: ' + totalQuery + ', ' + Math.round(totalQuery / totalTime, 3) * 100);
        console.log('Unique users: ' + uniqueUsers.length + '\n');
    },

    generateRequestReport: function (processedRequests) {
        var that = this;
        processedRequests.forEach(function (r) {
            if (r[0].request_duration > 3) {
                that.ppRequestQueries(r[1], r[0]);
                that.ppRequest(r[0]);
            }
        });
    },

    ppUserReports: function (request, title) {
        title = title || false;
        if (title) {
            console.log('User       Sales        Reach        SHolding     SMovement    Fillrate     FillrateD    Map          Total           Name');
        } else {
            if (!!request) {
                console.log(
                    request.user_id + this.whiteSpaces(10, String(request.user_id).length) + ' ' +
                        request.sales + this.whiteSpaces(12, String(request.sales).length) + ' ' +
                        request.reach + this.whiteSpaces(12, String(request.reach).length) + ' ' +
                        request.stockholding + this.whiteSpaces(12, String(request.stockholding).length) + ' ' +
                        request.stock_movement + this.whiteSpaces(12, String(request.stock_movement).length) + ' ' +
                        request.fillrate + this.whiteSpaces(12, String(request.fillrate).length) + ' ' +
                        request.fillrate_diagnostics + this.whiteSpaces(12, String(request.fillrate_diagnostics).length) + ' ' +
                        request.retailer_maps + this.whiteSpaces(12, String(request.retailer_maps).length) + ' ' +
                        request.query_count + this.whiteSpaces(12, String(request.query_count).length) + '    ' +
                        request.name
                );
            }
        }
    },

    generateUserReports: function (processedRequests, usernames) {
        var d, users = [], name = [], that = this;
        Object.keys(processedRequests).forEach(function (c) {
            d = processedRequests[c];
            if (users.indexOf(d[0].user_id) === -1) {
                Object.keys(usernames).forEach(function (i) {
                    if ('[' + usernames[i].id + ']' === d[0].user_id) {
                        name.push(usernames[i].username);
                    }
                });
                users[d[0].user_id] = {
                    'user_id': d[0].user_id,
                    'name': (name.length === 1 ? name[0] : ''),
                    'sales': 0,
                    'reach': 0,
                    'stockholding': 0,
                    'stock_movement': 0,
                    'fillrate': 0,
                    'fillrate_diagnostics': 0,
                    'retailer_maps': 0,
                    'query_count': 0
                };
            }

            switch (d[1][0].name) {
            case 'SALES_DASHBOARD_BETA_SALES_QUERY':
                users[d[0].user_id].sales += 1;
                break;
            case 'REACH_DASHBOARD_REACH_COUNT_QUERY':
                users[d[0].user_id].reach += 1;
                break;
            case 'STOCKHOLDING_DASHBOARD_QUERY':
                users[d[0].user_id].stockholding += 1;
                break;
            case 'FILLRATE_DASHBOARD_QUERY':
                users[d[0].user_id].fillrate += 1;
                break;
            case 'FILLRATE_DIAGNOSTICS_QUERY':
                users[d[0].user_id].fillrate_diagnostics += 1;
                break;
            case 'STOCK_MOVEMENT_DASHBOARD_QUERY':
                users[d[0].user_id].stock_movement += 1;
                break;
            case 'RETAILER_MAPS_QUERY2':
                users[d[0].user_id].retailer_maps += 1;
                break;
            }
            users[d[0].user_id] = users[d[0].user_id] || {};
            users[d[0].user_id].query_count = users[d[0].user_id].query_count || [];
            users[d[0].user_id].query_count += 1;
        });

        this.ppUserReports(null, true);

        Object.keys(users).forEach(function (i) {
            that.ppUserReports(users[i]);
        });
    },

    generateCacheReport: function (cacheKeys) {
        var k, l;
        for (k in cacheKeys) {
            if (cacheKeys.hasOwnProperty(k)) {
                // only print the results if there is more then one request for the same
                // cache key
                if (cacheKeys[k].length > 1) {
                    console.log(k, '\n');
                    for (l in cacheKeys[k].sort()) {
                        if (cacheKeys[k].sort().hasOwnProperty(l)) {
                            console.log(
                                cacheKeys[k][l][0] + this.whiteSpaces(18, String(cacheKeys[k][l][0]).length) + ' ' +
                                    cacheKeys[k][l][1] + ' ' +
                                    cacheKeys[k][l][2] + this.whiteSpaces(8, String(cacheKeys[k][l][2]).length) + ' ' +
                                    cacheKeys[k][l][3]
                            );
                        }
                    }
                    console.log('\n\n');
                }
            }
        }
    },

    generateProcessedRequest: function (lines) {
        var cacheKeys = {}, // Data structure used to analyse cache effectiveness
            group = {},
            uniqueUsers = [],
            processedRequests = [], // Stores [{request_details}, [{request_query_details}, ]]
            requestLogLines = [],
            foundStart = false,
            requestQueries,
            that = this,
            sortKey = ['user_id', 'processid', 'threadid', 'date', 'time']; // Sort by userid, processid, threadid, date, time

        this.sorted(lines, sortKey);
        lines.forEach(function (l) {
            if (!group[l.user_id]) { group[l.user_id] = {}; }
            if (!group[l.user_id][l.processid]) { group[l.user_id][l.processid] = {}; }
            if (!group[l.user_id][l.processid][l.threadid]) { group[l.user_id][l.processid][l.threadid] = []; }
            group[l.user_id][l.processid][l.threadid].push(l);

            if (uniqueUsers.indexOf(l.user_id) === -1) {
                uniqueUsers.push(l.user_id);
            }
        });

        uniqueUsers.forEach(function (user) {
            Object.keys(group[user]).forEach(function (j) {
                Object.keys(group[user][j]).forEach(function (k) {
                    group[user][j][k].forEach(function (row) {
                        var tmp;
                        if (row.log.search('json_report_export started') !== -1) {
                            foundStart = true;
                        }

                        if (foundStart) {
                            requestLogLines.push(row);
                        }

                        if (row.log.search('json_report_export took') !== -1 && requestLogLines.length !== 0) {
                            requestQueries = that.getRequestQueries(requestLogLines);
                            Object.keys(requestQueries).forEach(function (r) {
                                if (!cacheKeys[requestQueries[r].cache_key]) {
                                    cacheKeys[requestQueries[r].cache_key] = [];
                                }
                                cacheKeys[requestQueries[r].cache_key].push([
                                    requestQueries[r].cache_start_time,
                                    requestQueries[r].from_cache,
                                    requestQueries[r].query_duration,
                                    row.user_id,
                                    row.processid,
                                    row.threadid]);
                            });
                            tmp = [that.processRequest(requestLogLines, requestQueries), requestQueries];
                            if (!processedRequests.hasOwnProperty(tmp)) {
                                processedRequests.push(tmp);
                            }
                            requestLogLines = [];
                            foundStart = false;
                        }
                    });
                });
            });
        });
        return [processedRequests, cacheKeys];
    },

    whiteSpaces: function (space, minus) {
        var i, spaces = '';
        minus = minus || 0;
        space = space || 0;
        if (space - minus > 0) {
            for (i = 0; i < space - minus; i += 1) {
                spaces += ' ';
            }
        }
        return spaces;
    },

    sorted : function (array, keys, reverse) {
        reverse = reverse || false;
        return array.sort(function (a, b) {
            var tmp = 0, i = 0, j, k;
            while (tmp === 0 && i < keys.length) {
                j = a[keys[i]].toLowerCase();
                k = b[keys[i]].toLowerCase();
                tmp = j > k ? 1 : -1;
                if (j === k) {
                    tmp = 0;
                }
                i += 1;
            }
            if (reverse) {
                return tmp.reverse();
            }
            return tmp;
        });
    }
};

module.exports = LogParsing;

if (require.main === module) {
    program
        .version('0.0.1')
        .usage('[options] [file path]')
        .option('-c, --cache-report', 'Include the cache report in the output')
        .parse(process.argv);

    var cacheReport = program.args[0],
        lines = [],
        arrays = [],
        users = [],
        i,
        gpr,
        processedRequests,
        cacheKeys,
        lp = new LogParsing();

    try {
        arrays = fs.readFileSync(cacheReport).toString().split('\n');
        for (i in arrays) {
            if (arrays.hasOwnProperty(i) && arrays[i].length > 0) {
                lines.push(lp.parseDs3LogLine(arrays[i]));
            }
        }
    } catch (e) {
        throw ('Cache report not found: ' + e);
    }
    gpr = lp.generateProcessedRequest(lines);
    processedRequests = gpr[0];
    cacheKeys = gpr[1];
    console.log('Filename: ', cacheReport);
    lp.generateRequestTotalReport(processedRequests);

    console.log('Total query count by users for each dashboard:');

    lp.getUsernameList(function (user) {
        lp.generateUserReports(processedRequests, user.rows);

        // Sort requests by duration of time taken
        lp.sorted(processedRequests, processedRequests.map(function (r) {
            return r.request_duration;
        }));

        console.log('\n');
        console.log('Queries that took more than 3s: ');
        if (processedRequests) {
            lp.generateRequestReport(processedRequests);
        } else {
            console.error('Empty cache report encountered');
        }

        if (!!program.cacheReport) {
            lp.generateCacheReport(cacheKeys);
        }
    });

}
