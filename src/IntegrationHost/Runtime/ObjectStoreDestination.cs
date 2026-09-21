using System.Text;
using System.Text.RegularExpressions;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IntegrationHost.Pipes;

namespace IntegrationHost.Runtime;

/// <summary>Where objects are written. <see cref="PutIfAbsentAsync"/> is the idempotency point: it never overwrites.</summary>
public interface IObjectStore
{
    /// <summary>Returns false (without error) if an object with this name already exists — a redelivery.</summary>
    Task<bool> PutIfAbsentAsync(string name, byte[] body, CancellationToken ct);
}

/// <summary>
/// Writes each message as one object, named from a template. Because the name contains the source message id and existing
/// objects are never overwritten, an at-least-once redelivery is a no-op rather than a duplicate or a corruption.
/// </summary>
public sealed partial class ObjectStoreDestination(IObjectStore store, DestinationConfig config) : IDestination
{
    public async Task SendAsync(Envelope message, CancellationToken ct) =>
        await store.PutIfAbsentAsync(Name(message), Encoding.UTF8.GetBytes(message.Payload), ct);

    internal string Name(Envelope m)
    {
        var type = m.Type ?? "";
        if (config.StripTypePrefix.Length > 0 && type.StartsWith(config.StripTypePrefix, StringComparison.Ordinal))
            type = type[config.StripTypePrefix.Length..];
        return Token().Replace(config.NameTemplate, t => t.Groups[1].Value switch
        {
            "id" => m.Id,
            "type" => type,
            "correlationId" => m.Headers.GetValueOrDefault("correlationId") ?? m.Headers.GetValueOrDefault("correlation_id") ?? "",
            "partitionKey" => m.Headers.GetValueOrDefault("partitionKey") ?? m.Headers.GetValueOrDefault("partition_key") ?? "",
            "date" => DateTime.UtcNow.ToString("yyyy/MM/dd"),
            _ => t.Value,
        });
    }

    [GeneratedRegex(@"\{([^{}]*)\}")]
    private static partial Regex Token();
}

/// <summary>Azure Blob container. Created on first use. Names are used exactly as generated.</summary>
public sealed class AzureBlobStore(DestinationConfig config) : IObjectStore
{
    private readonly SemaphoreSlim gate = new(1, 1);
    private BlobContainerClient? container;

    private async Task<BlobContainerClient> ContainerAsync(CancellationToken ct)
    {
        if (container is not null) return container;
        await gate.WaitAsync(ct);
        try
        {
            if (container is not null) return container;
            var c = string.IsNullOrWhiteSpace(config.ServiceUri)
                ? new BlobContainerClient(config.ConnectionString, config.Container)
                : new BlobServiceClient(new Uri(config.ServiceUri), new DefaultAzureCredential()).GetBlobContainerClient(config.Container);
            await c.CreateIfNotExistsAsync(cancellationToken: ct);
            return container = c;
        }
        finally { gate.Release(); }
    }

    public async Task<bool> PutIfAbsentAsync(string name, byte[] body, CancellationToken ct)
    {
        var blob = (await ContainerAsync(ct)).GetBlobClient(name);
        try
        {
            using var ms = new MemoryStream(body, writable: false);
            await blob.UploadAsync(ms, new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, ct);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status is 409 or 412)
        {
            return false; // already exists: this is a redelivery
        }
    }
}

/// <summary>Local directory (or any mounted volume). Names are sanitised so message data can never escape the root.</summary>
public sealed class FileStore(string root) : IObjectStore
{
    private readonly string root = Path.GetFullPath(root);

    public async Task<bool> PutIfAbsentAsync(string name, byte[] body, CancellationToken ct)
    {
        var path = Resolve(name);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        try
        {
            await using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            await fs.WriteAsync(body, ct);
            return true;
        }
        catch (IOException) when (File.Exists(path))
        {
            return false;
        }
    }

    private string Resolve(string name)
    {
        // '/' is allowed (it creates subdirectories, e.g. from {date}); anything that could climb out is neutralised.
        var parts = name.Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Select(p => new string(p.Select(c => Path.GetInvalidFileNameChars().Contains(c) ? '_' : c).ToArray()))
            .Select(p => p is "." or ".." ? "_" : p);
        var full = Path.GetFullPath(Path.Combine([root, .. parts]));
        if (!full.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidOperationException($"Object name '{name}' resolves outside the store root.");
        return full;
    }
}
