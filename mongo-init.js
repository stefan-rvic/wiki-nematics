db = db.getSiblingDB('wiki_stream');

db.createCollection('COUNT');

db.createCollection('CHANGES', {
    timeseries: {
        timeField: "dt",
        granularity: "seconds"
    }
});
