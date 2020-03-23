'use strict'
const axios = require('axios')
const process = require('process')
require('array.prototype.flatmap').shim()

const { Client } = require('elasticsearch')
const client = new Client({
  hosts: process.env.ES_ADDON_URI || 'http://localhost:9200'
})
const express = require('express')
const app = express();
app.get('/', (req, res) => {
    res.send('Hello !');
});
app.listen(8080, () => console.log('Listening on port 8080!')); 


const localapp = express();
localapp.get('/', (req, res) => {
  for (const city in meetups) {
    meetups[city].forEach(meetupName => {
        axios.get(`https://api.meetup.com/${meetupName}/events/?status=past,upcoming\&fields=comment_count`).then((response) => {
            let dataset = response.data.map(meetup => {
                meetup["city"] = city;
                if (meetup.group && meetup.group.lat)  meetup["grouploc"] = {"lat":meetup.group.lat,"lon":meetup.group.lon};
                if (meetup.venue && meetup.venue.lat) meetup["venueloc"] = {"lat":meetup.venue.lat,"lon":meetup.venue.lon};
                return meetup;
            })
            bulkInsert(dataset).catch(console.log);
            }
        );
    });
  }
    res.send('running import !');
});
localapp.listen(8081, 'localhost', function() {
  console.log("... port %d in %s mode", 8081, localapp.settings.env);
});



let meetups = require('./meetups.json');


async function bulkInsert (dataset) {
  
    try {
        await client.indices.create({
            index: "meetup",
            body : {
                "mappings": {
                    "properties": {
                      "time":  { "type": "date","format": "epoch_millis"  }, 
                      "group.name": {"type": "keyword",},
                      "yes_rsvp_count" : {"type": "integer",},
                      "grouploc": {"type": "geo_point"},
                      "venueloc":{"type": "geo_point"}
                    }
                  }
            }
        });
    } catch (e) {
        console.log(e);
    }
    const body = dataset.flatMap(doc => [{ index: { _index: 'meetup' } }, doc])
  
    const bulkResponse  = await client.bulk({ refresh: true, body })
  
    if (bulkResponse.errors) {
      const erroredDocuments = []
      // The items array has the same order of the dataset we just indexed.
      // The presence of the `error` key indicates that the operation
      // that we did for the document has failed.
      bulkResponse.items.forEach((action, i) => {
        const operation = Object.keys(action)[0]
        if (action[operation].error) {
          erroredDocuments.push({
            // If the status is 429 it means that you can retry the document,
            // otherwise it's very likely a mapping error, and you should
            // fix the document before to try it again.
            status: action[operation].status,
            error: action[operation].error,
            operation: body[i * 2],
            document: body[i * 2 + 1]
          })
        }
      })
      console.log(erroredDocuments)
    }
  }

