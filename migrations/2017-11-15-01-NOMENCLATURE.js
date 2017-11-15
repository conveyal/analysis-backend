// 1 Projects -> Regions
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
