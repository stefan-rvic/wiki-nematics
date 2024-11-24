db = db.getSiblingDB('wiki_stream');

db.createCollection('CHANGES', {
    timeseries: {
        timeField: "dt",
        granularity: "seconds"
    }
});
