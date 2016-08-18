// convert add-stops modifications to reroute modifications
db.modifications.update({ 'type': { '$eq': 'add-stops' }}, { '$set': { 'type': 'reroute' }}, { multi: true })