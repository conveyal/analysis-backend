// Extract all ODs from Region objects and create them on their own
db.regions.find({}).forEach(function (r) {
  r.opportunityDatasets.forEach(function (od) {
    db.opportunityDatasets.save({
      _id: (new ObjectId).valueOf(), // force it to be a string
      accessGroup: r.accessGroup,
      createdBy: r.createdBy,
      updatedBy: r.updatedBy,
      createdAt: NumberLong(Date.now()),
      updatedAt: NumberLong(Date.now()),
      name: od.name,
      key: od.key,
      sourceName: od.dataSource,
      sourceId: od.dataSource + r._id,
      bucketName: 'analysis-staging-grids', // TODO: Must be changed for prod
      regionId: r._id
    })
  })

  db.regions.update({'_id': r._id}, {$unset: {opportunityDatasets: ''}});
})
