/* global db */
/* Convert single speeds to segment speeds */

const reroutes = db.modifications.find({ type: { $eq: 'reroute' } }).toArray()
reroutes.forEach(rr => {
  if (!(rr.speed instanceof Array)) {
    rr.speed = rr.segments.map(s => rr.speed)
    db.modifications.save(rr)
  }
})

const addTrips = db.modifications.find({ type: { $eq: 'add-trip-pattern' } }).toArray()
addTrips.forEach(at => {
  var modified = false
  at.timetables.forEach(tt => {
    if (!(tt.speed instanceof Array)) {
      tt.speed = at.segments.map(s => tt.speed)
      modified = true
    }
  })

  if (modified) {
    db.modifications.save(at)
  }
})
