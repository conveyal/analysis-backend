// 1 Migrate all createdAt/updatedAt to appropriate Date objects (or add them if they did not exist)
// Also, handle the migration of `creationTime` from regional analyses while we are at it.
db
	.getCollectionNames()
	.map(function (name) { return db.getCollection(name); })
	.forEach(function (collection) {
		collection.find({}).forEach(function (entry) { 
			if (entry.creationTime) { // For regional analyses
				entry.createdAt = new Date(entry.creationTime);
				delete entry.creationTime;
			} else {
				entry.createdAt = entry.createdAt ? new Date(entry.createdAt) : new Date();
			}

			entry.updatedAt = entry.updatedAt ? new Date(entry.updatedAt) : new Date();
			collection.save(entry);	
		});
	});

// 2 Remove all r5 entries from Projects 
db.projects.update({}, {$unset: {r5Version: ""}}, {multi: true});


