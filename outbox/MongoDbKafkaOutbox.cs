using MongoDB.Bson;
using MongoDB.Driver;

namespace outbox;

// The idea of the metrics
// We will see enqueued for all pods and processed from the replica
// Hence we can calculate our processing speed and also reason about the latency
// For the lag we must have a grafana alert because that means we can't catch up
// The size of the collection is just an information (since we configure the size in GB and don't know the amount of docs)
// We might use it for tuning

public class MongoDbKafkaOutbox : IOutbox
{
    private long _enqueued = 0; // Expose this as metric
    private long _processed = 0; // Expose this as metric
    private readonly OutboxWorker _outboxWorker;
    private readonly IMongoCollection<BsonDocument> _outboxCollection;
    private const string OutboxOffsetCollectionName = "outboxOffsets";

    public MongoDbKafkaOutbox(IMongoDatabase database, string outboxCollectionName)
    {
        _outboxCollection = database.GetCollection<BsonDocument>(outboxCollectionName);
        _outboxWorker = new OutboxWorker(database, outboxCollectionName, OutboxOffsetCollectionName);
    }

    private async Task ProcessRecord(BsonDocument record)
    {
        await Task.Delay(10); // Simulate some processing delay
        Console.WriteLine($"Processed record {record["_id"]}");
    }

    public Task<long> GetLag() => _outboxWorker.GetLag(); // Expose this as metric

    public Task Enqueue(BsonDocument record)
    {
        _enqueued++;
        return _outboxCollection.InsertOneAsync(record);
    }
    
    public Task StartProcessor(CancellationToken cancellationToken)
    {
        _processed++;
        return _outboxWorker.RunAsync(cancellationToken, ProcessRecord);
    }

    public long Length => _outboxCollection.CountDocuments(FilterDefinition<BsonDocument>.Empty); // Expose this as metric
}