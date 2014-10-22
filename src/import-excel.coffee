# This script imports data from xlsx file provided by UOKiK into ElasticSearch
#
# The file can be downloaded from <http://uokik.gov.pl/download.php?id=1064>
#
# TODO: download xlsx file
# TODO: prepare index

async   = require "async"
excel   = require "excel"
_       = require "lodash"
moment  = require "moment"
elastic = require "elasticsearch"

es      = new elastic.Client
  host: process.env.ELASTICSEARCH_URL or 'http://localhost:9200'
  log : "trace"
-  
keys = [
  '_id'
  'court_date'  
  'court_sign' 
  'court'
  'plaintiffs'
  'defendants'
  'text'
  'register_date'
  'notes'
  'market'
]
get_date = (value) -> 
  moment '1900-01-01'
    .add 'days', Math.floor value - 1
    .toDate()

transformations = 
  'court_date'    : get_date
  'register_date' : get_date

console.log "Importing. This can take several minutes..."

async.waterfall [
  (done) -> es.ping requestTimeout: 1000, (error) -> done error
  # TODO: run next steps only if reset command line option is set
  (done) -> es.indices.exists index: 'ab2c', done
  (exists, status, done) -> 
    if exists then es.indices.delete index: 'ab2c', done
    else done null
  (msg, status, done) ->
    console.log "creating index"
    es.indices.create 
      index   : 'ab2c'
      body    :
        mappings:
          term    :
            properties:
              court     :
                type      : "string"
              court_date:
                type      : "date"
                format    : "dateOptionalTime"
              court_sign:
                type      : "string"
              defendants:
                type      : "string"
              market    : 
                type      : "string"
              notes     :
                type      : "string"
              plaintiffs:
                type      : "string"
              register_date:
                 type     : "date"
                 format   : "dateOptionalTime"
              text      :
                 type     : "string"
                 analyzer : "polish"
      done

  (msg, status, done) ->
    console.log "Index and mapping are set"
    done null
  (done) -> 
    console.log "Parsing excel file"
    excel "./rejestr.xlsx", done
  (data, done) ->
    console.log "Processing %d rows", data.length
    do data.shift # first row is a header
    async.eachLimit data, 10,
      (row, done) ->
        if not row[0] then return setImmediate -> done null
        values = row[...keys.length]
        document = _.zipObject keys, values
        for key, fn of transformations
          document[key] = fn document[key]

        es.index
          index : 'ab2c'
          type  : 'term'
          id    : document._id
          body  : document
          (error) ->
            console.log "done inserting %d", document._id
            done error
      done
], (error) ->
  if error then throw error
  console.log "Fine."
  process.exit 0


  
