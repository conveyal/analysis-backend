/* global db */
/* Convert single speeds to segment speeds */

const reroutes = db.modifications.find({ type: { $eq: 'reroute' } }).toArray()
reroutes.forEach(rr => {
  if (!(rr.segmentSpeeds instanceof Array)) {
    rr.segmentSpeeds = rr.segments.map(s => rr.speed)
    delete rr.speed
    db.modifications.save(rr)
  }
})

const addTrips = db.modifications.find({ type: { $eq: 'add-trip-pattern' } }).toArray()
addTrips.forEach(at => {
  var modified = false
  at.timetables.forEach(tt => {
    if (!(tt.segmentSpeeds instanceof Array)) {
      tt.segmentSpeeds = at.segments.map(s => tt.speed)
      delete tt.speed
      modified = true
    }
  })

  if (modified) {
    db.modifications.save(at)
  }
})
