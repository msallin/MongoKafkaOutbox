using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

public class OutboxWorker
{
    private const string IdFieldName = "_id";

    private readonly Lazy<bool> _runs = new(() => false);

    private readonly IMongoCollection<BsonDocument> _outboxCollection;

    private readonly IMongoCollection<BsonDocument> _outboxOffsetCollection;

    private readonly FilterDefinition<BsonDocument> _outboxOffsetRecordFilter;

    private readonly BatchOffsetHolder _batchOffset = new();

    public OutboxWorker(IMongoDatabase database, string outboxCollectionName, string outboxOffsetCollectionName)
    {
        _outboxCollection = database.GetCollection<BsonDocument>(outboxCollectionName);
        _outboxOffsetCollection = database.GetCollection<BsonDocument>(outboxOffsetCollectionName);
        _outboxOffsetRecordFilter = Builders<BsonDocument>.Filter.Eq(IdFieldName, outboxCollectionName);
    }

    public async Task RunAsync(CancellationToken cancellationToken, Func<BsonDocument, Task> processMessageAsync)
    {
        Console.WriteLine("[INFO] Starting outbox worker");

        // Ensure mutual exclusion. This class isn't thread-safe by design.
        if (_runs.Value)
        {
            throw new InvalidOperationException("The outbox worker is already running.");
        }

        using var cursor = await GetCursorFromLastPosition(cancellationToken);

        try
        {
            while (await cursor.MoveNextAsync(cancellationToken))
            {
                Console.WriteLine("[DEBUG] Got cursor results");

                // The cursor returns the documents in batches.
                foreach (var document in cursor.Current)
                {
                    Console.WriteLine($"[DEBUG] Process document: {document}");

                    await processMessageAsync(document);
                    _batchOffset.Increment();

                    // Always remember the last processed document.
                    // This will be committed on graceful shutdown or if the batch was processed.
                    _batchOffset.Push(document[IdFieldName].AsObjectId);
                }

                Console.WriteLine($"[DEBUG] Processed documents: {(int)_batchOffset}, time taken: xy");

                // We guarantee at least once. Thus, it's fine to commit only the whole batch.
                // In a disgraceful shutdown case we might resend some records.
                // The last position is null when the cursor returns from the database but there are 
                // no new records. Then we don't need to commit the same offset again.
                // Thus, we set it to null after a successful commit.
                if (_batchOffset)
                {
                    await CommitPosition(_batchOffset.Pop());
                }
            }
        }
        // Do nothing but let all other exceptions bubble up.
        catch (OperationCanceledException)
        {
        }
        finally
        {
            // We reach this position when the cancellationToken was cancelled.
            // Or when an exception was thrown due to an error.
            // Always make sure to commit the last known position.
            if (_batchOffset)
            {
                await CommitPosition(_batchOffset.Pop());
            }

            Console.WriteLine("[INFO] Stopped outbox worker");
        }
    }
    
    public Task<long> GetLag() => GetTail(CancellationToken.None, _batchOffset.Last());

    private async Task<IAsyncCursor<BsonDocument>> GetCursorFromLastPosition(CancellationToken cancellationToken)
    {
        // There is a none intuitive behavior of mongodb.
        // When the capped collection does not contain any records, the cursor is considered "dead".
        // This doesn't happen when there are documents but the query returns no direct results.
        // To prevent that we always insert a dummy document if the collection is empty.
        // Notice that Count is efficient, it doesn't enumerate a capped collection but uses metadata.
        long count = await _outboxCollection.CountDocumentsAsync(Builders<BsonDocument>.Filter.Empty,
            cancellationToken: cancellationToken);
        if (count == 0)
        {
            // This happens exactly one time in the lifetime of the capped collection.
            // We accept that the program might fail between InsertOneAsync and the commit
            // of exactly that record. In that case we send invalid data out of the outbox.
            // We could filter, but it's a perf hit and not worth it.
            BsonDocument dummyDocument = new();
            await _outboxCollection.InsertOneAsync(dummyDocument, cancellationToken: cancellationToken);

            // In case we inserted a dummy document we never want to process it.
            // Hence, we set the last offset to the id of this dummy document.
            // Since we are using a greater than filter this doc will be skipped.
            await CommitPosition(dummyDocument[IdFieldName]);
            Console.WriteLine($"[INFO] Collection is empty, inserted a dummy document: {dummyDocument[IdFieldName]}.");
        }

        BsonObjectId lastOffset = await GetLastCommitedOffset();

        var docFound = await Exists(cancellationToken, lastOffset);
        if (!docFound)
        {
            Console.WriteLine("[WARN] The document for the last offset was not found. That could mean data was lost.");
        }

        var lag = await GetTail(cancellationToken, lastOffset);
        Console.WriteLine($"[INFO] Starting lag: {lag}");

        IAsyncCursor<BsonDocument>? cursor = null;
        try
        {
            // Fetches the last commited offset or non (min key value) and creates a filter.
            // The filter/offset is the _id which is a clustered index and an ordinal scale value.
            // This the tailed cursor will quickly find where to continue from.
            FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Gt(IdFieldName, lastOffset);
            FindOptions<BsonDocument> options = new()
            {
                CursorType = CursorType.TailableAwait, // Means it waits for new records when reaching the end of the collection
                BatchSize = 100, // This is the max size not the min size, should be inline with what send
                NoCursorTimeout = true, // Should not be terminated
                MaxAwaitTime = TimeSpan.FromSeconds(10) // Wait until the cursor returns (also with empty values)
            };

            return await _outboxCollection.FindAsync(filter, options, cancellationToken);
        }
        catch
        {
            cursor?.Dispose();
            throw;
        }
    }

    private async Task<bool> Exists(CancellationToken cancellationToken, BsonObjectId lastOffset)
    {
        // A capped collection is a circular buffer and the oldest records are dropped when the configured size is reached.
        // That means when we do not find the last committed offset in the capped collection this record is deleted.
        // That might indicate that we lost data. We can't do anything about it but a warning must be written.
        FilterDefinition<BsonDocument>? documentForOffset = Builders<BsonDocument>.Filter.Eq(IdFieldName, lastOffset);
        return await _outboxCollection.Find(documentForOffset).Limit(1).CountDocumentsAsync(cancellationToken) == 1;
    }

    public async Task<long> GetTail(CancellationToken cancellationToken, BsonObjectId lastOffset)
    {
        // When we start the outbox processing we want to know how big the lag is.
        // The id is monotony growing thus this also works when the record with the latest offset was deleted.
        // In that case the lag is just the number of all records in the capped collection.
        FilterDefinition<BsonDocument>? lagFilter = Builders<BsonDocument>.Filter.Gt(IdFieldName, lastOffset);
        return await _outboxCollection.Find(lagFilter).CountDocumentsAsync(cancellationToken);
    }

    private async Task<BsonObjectId> GetLastCommitedOffset()
    {
        BsonDocument? commitedOffset = await _outboxOffsetCollection.Find(_outboxOffsetRecordFilter).SingleOrDefaultAsync();
        if (commitedOffset == null)
        {
            Console.WriteLine($"[INFO] No offset commited. Using {BsonObjectId.Empty}");
            return BsonObjectId.Empty;
        }

        var offset = commitedOffset["offset"];
        var offsetTimestamp = commitedOffset["timestamp"];
        var offsetMetadata = commitedOffset["metadata"];
        if (offset == null || offsetTimestamp == null)
        {
            throw new InvalidOperationException($"offset commit document invalid. Found: {commitedOffset}");
        }

        Console.WriteLine($"[INFO] Offset found: {offset}, from: {offsetTimestamp}, with metadata: {offsetMetadata}");
        return offset.AsObjectId;
    }

    private async Task CommitPosition(BsonValue offset)
    {
        // We want to update the record which contains the offset.
        // This is done by using the "_outboxOffsetRecordFilter" which does get this record by it's _id.
        // It is a clustered index which makes this operation very fast.
        // Then we add a filter that mitigates race conditions (that shouldn't happen but one never knows)
        // By ensure we only overwrite/update when the offset to be written is equal or greater than the offset in the database.
        Builders<BsonDocument>.Filter.And(_outboxOffsetRecordFilter, Builders<BsonDocument>.Filter.Gte(IdFieldName, "offset"));
        
        var update = Builders<BsonDocument>.Update
            .Set("offset", offset)
            .Set("timestamp", DateTime.UtcNow)
            .Set("metadata", new BsonDocument
            {
                { "somethingUseful1", BsonMinKey.Value },
                { "somethingUseful2", BsonMinKey.Value }
            });

        Console.WriteLine($"[DEBUG] Commiting offset {update}");

        // We need no filter since there is only one document in this collection.
        // The first time we do that, there is no document, hence we do an Upsert.
        UpdateResult result = await _outboxOffsetCollection.UpdateOneAsync(_outboxOffsetRecordFilter, update, new UpdateOptions { IsUpsert = true  });
        if (result.ModifiedCount < 1)
        {
            Console.WriteLine("[WARN] Offset not updated due to a race condition");
        }

        Console.WriteLine("[DEBUG] Committed offset. Time taken: xy ms");
    }


    private class BatchOffsetHolder
    {
        private BsonObjectId _value = null!;

        private bool _isSet;

        private int _processedCounter;

        public BsonObjectId Pop()
        {
            if (!_isSet)
            {
                throw new InvalidOperationException("Cannot pop until the offset has been set.");
            }

            _isSet = false;
            _processedCounter = 0;
            return _value;
        }

        public BsonObjectId Last()
        {
            return _value;
        }

        public void Push(BsonObjectId document)
        {
            _value = document;
            _isSet = true;
        }

        public static implicit operator bool(BatchOffsetHolder s) => s._isSet;
        public static implicit operator int(BatchOffsetHolder s) => s._processedCounter;

        public void Increment()
        {
            _processedCounter++;
        }
    }
}