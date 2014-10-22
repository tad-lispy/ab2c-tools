async   = require "async"
_       = require "lodash"
elastic = require "elasticsearch"

es      = new elastic.Client
  host: 'localhost:9200'
  # log : 'trace'

search = process.argv[2...].join ' '

console.log "Looking for '#{search}'..."

es.search
  index   : 'ab2c'
  type    : 'term'
  body    :
    query   :
      match   :
         text   : search

  (error, result) ->
    if error then throw error
    
    console.log """
      #{result.hits.total} hits

    """
    for hit in result.hits.hits
      console.log hit._source.text
      console.log "(#{hit._score})"
      console.log ""
    process.exit 0