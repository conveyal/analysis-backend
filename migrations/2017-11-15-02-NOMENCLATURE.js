// Rename Scenarios -> Projects
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
