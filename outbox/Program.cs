using MongoDB.Bson;
using MongoDB.Driver;
using outbox;

class Program
{
    private static IMongoClient _client;
    private static IMongoDatabase _database;

    private static async Task Main(string[] args)
    {
        _client = new MongoClient("mongodb://localhost:27017");
        _database = _client.GetDatabase("outboxPoc");
        
        await EnsureOutboxCollection("ProcessEvents");
        await EnsureOutboxCollection("TrackEvents");
        await EnsureOutboxOffsetCollection("outboxOffsets");

        IOutbox outbox1 = new MongoDbKafkaOutbox(_database, "ProcessEvents");
        IOutbox outbox2 = new MongoDbKafkaOutbox(_database, "TrackEvents");

        // This cts also needs to be canceled when leader election elects other leader.
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        await Task.WhenAll([
            outbox1.StartProcessor(cts.Token),
            outbox2.StartProcessor(cts.Token)
        ]);
    }

    
    /// <summary>
    ///     We create a capped collection (a circular buffer type of collection) for the outbox.
    ///     That gives us fast writes, guaranteed ordering (FIFO) and low latency
    ///     for reading by using a tailable cursor.
    /// </summary>
    private static async Task EnsureOutboxCollection(string name)
    {
        var filter = new BsonDocument("name", name);
        var collections = await _database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
        if (collections.Any())
        {
            return;
        }
        
        // Capped collections have an _id field and an index on the _id field by default.
        // Not sure if this is needed (we take a write performance hit).
        // And I also don't know how to disable...?
        var options = new CreateCollectionOptions<BsonDocument>
        {
            Capped = true,
            MaxSize = 1024, // size in bytes
        };

        await _database.CreateCollectionAsync(name, options);
    }

    /// <summary>
    ///     By default, the index "_index" is a normal index and not a clustered index.
    ///     We will only do a lookup records directly by the Id and hence we benefit of having
    ///     a clustered index.
    /// </summary>
    private static async Task EnsureOutboxOffsetCollection(string name)
    {
        var filter = new BsonDocument("name", name);
        var collections = await _database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
        if (collections.Any())
        {
            return;
        }

        var clusteredIndexOptions = new CreateCollectionOptions<BsonDocument>()
        {
            ClusteredIndex = new ClusteredIndexOptions<BsonDocument>
            {
                Key = new BsonDocument("_id", 1),
                Unique = true
            }
        };

        await _database.CreateCollectionAsync(name, clusteredIndexOptions);
    }
}
