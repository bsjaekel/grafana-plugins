// this a "legacy" datasource to support graphing metrics from OpenTSDB versions
// prior to the 2.x releases.
// most of the code has been copied over from the official opentsdb datasource
// and has been slightly modified to work with older OpenTSDB versions.

define([
  'angular',
  'lodash',
  'kbn',
  'moment',
  './queryCtrl',
],
function (angular, _, kbn) {
  'use strict';

  var module = angular.module('grafana.services');

  module.factory('OpenTSDBV1Datasource', function($q, backendSrv, templateSrv) {

    function OpenTSDBV1Datasource(datasource) {
      this.type = 'opentsdb';
      this.editorSrc = 'app/features/opentsdb/partials/query.editor.html';
      this.url = datasource.url;
      this.name = datasource.name;
      this.supportMetrics = true;
    }

    // Called once per panel (graph)
    OpenTSDBV1Datasource.prototype.query = function(options) {
      var start = convertToTSDBTime(options.range.from);
      var end = convertToTSDBTime(options.range.to);
      var qs = [];

      _.each(options.targets, function(target) {
        qs.push(convertTargetToQuery(target, options.interval));
      });

      var queries = _.compact(qs);

      // No valid targets, return the empty result to save a round trip.
      if (_.isEmpty(queries)) {
        var d = $q.defer();
        d.resolve({ data: [] });
        return d.promise;
      }

      var groupByTags = {};
      _.each(queries, function(query) {
        _.each(query.tags, function(val, key) {
          groupByTags[key] = true;
        });
      });

      return this.performTimeSeriesQuery(queries, start, end).then(function(response) {
        // the response.data field contains the queried metrics with one datapoint
        // per line as OpenTSDB V1 doesn't support returning the data as json.
        // Format is: "METRIC TIMESTAMP VALUE TAG1=TAGVALUE1 TAG2=TAGVALUE2..."
        var rows = response.data.split("\n"); // split to single rows
        var metricsData = {};

        _.each(rows, function(row) {
          // break up the line
          var parts = row.split(" ");

          // into its parts
          var metric = parts[0];
          var ts = parts[1];
          var val = parts[2];
          var rawTags = parts.slice(3);

          // to keep the work to a minimum (sorry for that), we rebuild the
          // OpenTSDB V2 result format from our V1 line based format
          var key = metric + ":" + rawTags.join(",");
          if(key in metricsData) { // if key is known, just save the data
              metricsData[key]["dps"][ts] = val;
          } else {
            // the key is unknown so we have a new metric to save our values to
            var tags = {};
            _.each(rawTags, function(tag) {
              var tagparts = tag.split("=");
              tags[tagparts[0]] = tagparts[1];
            });

            metricsData[key] = {
              "metric": metric,
              "dps": {ts: val},
              "tags": tags,
              "aggregatedTags": []
            };
          }
        });

        // we need an array of objects, not objects of object so put our generated
        // metrics data objects into an array
        var tsdbV2Data= [];
        for(var dataElem in metricsData) {
          tsdbV2Data.push(metricsData[dataElem]);
        }

        // now we can use our reformatted received data as input using all the code
        // from the official OpenTSDB plugin
        var metricToTargetMapping = mapMetricsToTargets(tsdbV2Data, options.targets);
        var result = _.map(tsdbV2Data, function(metricData, index) {
          index = metricToTargetMapping[index];
          return transformMetricData(metricData, groupByTags, options.targets[index]);
        });
        return { data: result };
      });
    };

    OpenTSDBV1Datasource.prototype.performTimeSeriesQuery = function(queries, start, end) {
      // instead of building the query as json objects tree, OpenTSDB v1 only
      // supports using the "q" endpoint with the whole query mangled into
      // appropriate query paramters.
      // we use the given queries to rebuild the full query url.
      // Template: AGGREGATOR<:DOWNSAMPLE><:RATE>:METRIC{TAG1=TAGVAL1,TAG2=TAGVAL2}
      var parts = queries.map(function(elem,idx) {
        var query = elem.aggregator + ":" + elem.downsample + ":";
        if("rate" in elem) {
          query = query + "rate:";
        }

        query = query + elem.metric;

        // check if any tags need to be appended to the metric
        if(_.size(elem.tags) > 0) {
          var tagsparts = $.map(elem.tags, function(value, key) {
            return key + "=" + value;
          });

          query = query + "{" + tagsparts.join(",") + "}";
        }

        return query;
      });

      // Relative queries (e.g. last hour) don't include an end time
      var requestParams = {
        m: parts,
        ascii: "",
        start: start
      };

      // add end parameter if any given
      if (end) {
        requestParams.end = end;
      }

      var options = {
        method: 'GET',
        url: this.url + "/q",
        params: requestParams
      };

      return backendSrv.datasourceRequest(options);
    };

    // just changed the url from /api/suggest to /suggest
    OpenTSDBV1Datasource.prototype.performSuggestQuery = function(query, type) {
      var options = {
        method: 'GET',
        url: this.url + '/suggest',
        params: {
          type: type,
          q: query
        }
      };
      return backendSrv.datasourceRequest(options).then(function(result) {
        return result.data;
      });
    };

    OpenTSDBV1Datasource.prototype.testDatasource = function() {
      return this.performSuggestQuery('cpu', 'metrics').then(function () {
        return { status: "success", message: "Data source is working", title: "Success" };
      });
    };

    function transformMetricData(md, groupByTags, options) {
      var metricLabel = createMetricLabel(md, options, groupByTags);
      var dps = [];

      // TSDB returns datapoints has a hash of ts => value.
      // Can't use _.pairs(invert()) because it stringifies keys/values
      _.each(md.dps, function (v, k) {
        dps.push([v, k * 1000]);
      });

      return { target: metricLabel, datapoints: dps };
    }

    function createMetricLabel(md, options, groupByTags) {
      if (!_.isUndefined(options) && options.alias) {
        var scopedVars = {};
        _.each(md.tags, function(value, key) {
          scopedVars['tag_' + key] = {value: value};
        });
        return templateSrv.replace(options.alias, scopedVars);
      }

      var label = md.metric;
      var tagData = [];

      if (!_.isEmpty(md.tags)) {
        _.each(_.pairs(md.tags), function(tag) {
          if (_.has(groupByTags, tag[0])) {
            tagData.push(tag[0] + "=" + tag[1]);
          }
        });
      }

      if (!_.isEmpty(tagData)) {
        label += "{" + tagData.join(", ") + "}";
      }

      return label;
    }

    function convertTargetToQuery(target, interval) {
      if (!target.metric || target.hide) {
        return null;
      }

      var query = {
        metric: templateSrv.replace(target.metric),
        aggregator: "avg"
      };

      if (target.aggregator) {
        query.aggregator = templateSrv.replace(target.aggregator);
      }

      if (target.shouldComputeRate) {
        query.rate = true;
        query.rateOptions = {
          counter: !!target.isCounter
        };

        if (target.counterMax && target.counterMax.length) {
          query.rateOptions.counterMax = parseInt(target.counterMax);
        }

        if (target.counterResetValue && target.counterResetValue.length) {
          query.rateOptions.resetValue = parseInt(target.counterResetValue);
        }
      }

      if (!target.disableDownsampling) {
        interval =  templateSrv.replace(target.downsampleInterval || interval);

        if (interval.match(/\.[0-9]+s/)) {
          interval = parseFloat(interval)*1000 + "ms";
        }

        query.downsample = interval + "-" + target.downsampleAggregator;
      }

      query.tags = angular.copy(target.tags);
      if(query.tags){
        for(var key in query.tags){
          query.tags[key] = templateSrv.replace(query.tags[key]);
        }
      }

      return query;
    }

    function mapMetricsToTargets(metrics, targets) {
      var interpolatedTagValue;
      return _.map(metrics, function(metricData) {
        return _.findIndex(targets, function(target) {
          return target.metric === metricData.metric &&
            _.all(target.tags, function(tagV, tagK) {
            interpolatedTagValue = templateSrv.replace(tagV);
            return metricData.tags[tagK] === interpolatedTagValue || interpolatedTagValue === "*";
          });
        });
      });
    }

    function convertToTSDBTime(date) {
      if (date === 'now') {
        return null;
      }

      date = kbn.parseDate(date);

      // we need a properly formatted opentsdb v1 datetime value using
      // the format YYYY/MM/DD-hh:mm:ss
      return date.getFullYear() + "/" + (date.getMonth() + 1) + "/" + date.getDate() + "-" + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds();
    }

    return OpenTSDBV1Datasource;
  });

});
