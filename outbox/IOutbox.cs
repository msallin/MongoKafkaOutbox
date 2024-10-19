using MongoDB.Bson;

namespace outbox;

public interface IOutbox
{
    Task<long> GetLag();
    Task Enqueue(BsonDocument record);
    Task StartProcessor(CancellationToken cancellationToken);
}