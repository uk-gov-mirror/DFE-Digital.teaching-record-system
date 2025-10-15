using Hangfire.Server;
using Microsoft.Extensions.Logging;
using TeachingRecordSystem.Core.DataStore.Postgres;
using TeachingRecordSystem.Core.DataStore.Postgres.Models;
using TeachingRecordSystem.Core.Services.TrsDataSync;

namespace TeachingRecordSystem.Core.Jobs;

public class CreateDqtContactAuditEventsJob(
    TrsDataSyncHelper syncHelper,
    IDbContextFactory<TrsDbContext> dbContextFactory,
    ILogger<CreateDqtContactAuditEventsJob> logger)
{
    public async Task ExecuteAsync(PerformContext context, CancellationToken cancellationToken)
    {
        var jobId = context.BackgroundJob.Id;
        var jobName = $"{nameof(SyncAllDqtAnnotationAuditsJob)}-{jobId}";

        await using var metadataDbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        metadataDbContext.Database.SetCommandTimeout(0);

        await using var readPersonsDbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        readPersonsDbContext.Database.SetCommandTimeout(0);

        var jobMetadata = await metadataDbContext.JobMetadata.SingleOrDefaultAsync(j => j.JobName == jobName, cancellationToken: cancellationToken);

        if (jobMetadata is null)
        {
            jobMetadata = new JobMetadata { JobName = jobName, Metadata = new() };
            metadataDbContext.JobMetadata.Add(jobMetadata);
            await metadataDbContext.SaveChangesAsync(cancellationToken);
        }

        var lastProcessedPersonId = jobMetadata.Metadata.TryGetValue(MetadataKeys.LastProcessedPersonId, out var lppIdStr)
            ? new Guid(lppIdStr)
            : Guid.Empty;

        int processed = 0;

        var personChunks = readPersonsDbContext.Persons
            .IgnoreQueryFilters()
            .Where(p => p.DqtContactId != null && p.PersonId > lastProcessedPersonId)
            .Select(p => p.PersonId)
            .OrderBy(id => id)
            .ToAsyncEnumerable()
            .ChunkAsync(250);

        await foreach (var personIds in personChunks.WithCancellation(cancellationToken))
        {
            await syncHelper.SyncPersonAuditsAsync(personIds, cancellationToken);

            processed += personIds.Length;

            if (processed % 10_000 == 0)
            {
                logger.LogInformation("Processed {Processed} persons", processed);

                jobMetadata.Metadata[MetadataKeys.LastProcessedPersonId] = personIds.Last().ToString();
                await metadataDbContext.SaveChangesAsync(cancellationToken);
            }
        }

        await metadataDbContext.JobMetadata.Where(j => j.JobName == jobName).ExecuteDeleteAsync();
    }

    private static class MetadataKeys
    {
        public const string LastProcessedPersonId = nameof(LastProcessedPersonId);
    }
}
