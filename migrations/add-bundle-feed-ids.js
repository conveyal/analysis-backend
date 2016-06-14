// Add IDs to bundle.feeds

db.bundles.find().forEach(function (b) {
  b.feeds.forEach(function (f) { f.id = f.feedId + '_' + b._id })
  db.bundles.save(b)
})