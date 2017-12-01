// 1 Rename project.group to project.accessGroup and set all project.statusCode to DONE
db.projects.update({},{
	$rename:{"group":"accessGroup"},
	$set:{"statusCode":"DONE"}
},{multi:true});

// 2 Rename `modifications.scenario` to `modifications.scenarioId`
db.modifications.update({},{$rename:{"scenario":"scenarioId"}},{multi:true});

// 3 Update all models with the `accessGroup` of their parent project
db.projects.find({}).forEach(function (p) {
	var accessGroup = p.accessGroup;
	var projectId = p._id;
	
	// Updated aggregation areas?
	if (db.aggregationAreas) db.aggregationAreas.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update bookmarks
	db.bookmarks.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi: true});

	// Update bundles
	db.bundles.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update regional analyses
	db.getCollection("regional-analyses").update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update scenarios and modifications
	db.scenarios.find({"projectId":projectId}).forEach(function (s) {
		var scenarioId = s._id;
		
		// Update modifications
		db.modifications.update({"scenarioId":scenarioId}, {$set:{"accessGroup":accessGroup}}, {multi: true});

		s.accessGroup = accessGroup;
		db.scenarios.save(s);
	});
});

// 4 Rename all timetable and frequency entry `id`s to `_id`
db.modifications.find({}).forEach(function (m) {
	if (m.timetables && m.timetables.length > 0) {
		m.timetables.forEach(function (tt) {
			tt._id = tt.id || tt.timetableId || ObjectId().valueOf();
			delete tt.id;
			delete tt.timetableId;
		});
		db.modifications.save(m);
	}

	if (m.entries && m.entries.length > 0) {
		m.entries.forEach(function (e) {
			e._id = e.id || e.entryId || ObjectId().valueOf();
			delete e.id;
			delete e.entryId;
		});
		db.modifications.save(m);
	}
});

// 5 Add nonces to all models that do not have them
// It's fine to set all items to have the same nonce as it just checks for the version within the specific item
function addNonceToCollection (collection) {
	collection.update({},{$set:{"nonce":ObjectId().valueOf()}},{multi:true});
}
if (db.aggregationAreas) addNonceToCollection(db.aggregationAreas);
addNonceToCollection(db.bookmarks);
addNonceToCollection(db.bundles);
addNonceToCollection(db.modifications);
addNonceToCollection(db.projects);
addNonceToCollection(db.getCollection("regional-analyses"));
addNonceToCollection(db.scenarios);

// 6 Migrate all createdAt/updatedAt to appropriate Date objects (or add them if they did not exist)
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

// 7 Remove all r5 entries from Projects
db.projects.update({}, {$unset: {r5Version: ""}}, {multi: true});

// WARNING THE FOLLOWING CAN ONLY BE PERFORMED ONCE. To prevent doing it twice, check for the existence of a "regions"
// collection.

if (!db.regions) {
    // 8 Projects -> Regions
    // Rename the collection, check for existance
    if (db.projects) {
        db.projects.renameCollection("regions");
    }
    // $rename: {"projectId": "regionId"}
    db
        .getCollectionNames()
        .map(function (name) { return db.getCollection(name); })
        .forEach(function (collection) {
            collection.update({}, {$rename:{projectId:"regionId"}}, {multi: true});
        });

    // 9 Rename Scenarios -> Projects
    if (db.scenarios) {
        db.scenarios.renameCollection("projects");
    }
    // $rename: {"scenarioId": "projectId"}
    db
        .getCollectionNames()
        .map(function (name) { return db.getCollection(name); })
        .forEach(function (collection) {
            collection.update({}, {$rename:{scenarioId:"projectId"}}, {multi: true});
        });
}
