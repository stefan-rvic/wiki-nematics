db = db.getSiblingDB('wiki_stream');

db.createCollection('CHANGES');

db.CHANGES.createIndex({ "dt": 1 }, {
    name: "dt_index",
    background: true
});
