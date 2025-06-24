using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// <see cref="JsonLogWriter"/> simplifies writing JSON-serialized messages to an <see cref="IWriteAheadLog"/>.
/// </summary>
public sealed class JsonLogWriter : IDisposable, IAsyncDisposable {

    private bool _disposed;
    
    private readonly LogWriter _writer;
    
    private readonly Utf8JsonWriter _jsonWriter;


    /// <summary>
    /// Creates a new <see cref="JsonLogWriter"/> instance.
    /// </summary>
    public JsonLogWriter() {
        _writer = new LogWriter();
        _jsonWriter = new Utf8JsonWriter(_writer);
    }


    /// <summary>
    /// Writes the specified data to the log as a JSON-serialized message.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to write the JSON message to.
    /// </param>
    /// <param name="data">
    ///   The data to serialize to JSON and write to the log.
    /// </param>
    /// <param name="options">
    ///   The <see cref="JsonSerializerOptions"/> to use for serialization.
    /// </param>
    /// <typeparam name="T">
    ///   The type of the data to serialize to JSON.
    /// </typeparam>
    /// <returns>
    ///   A <see cref="WriteResult"/> containing the sequence ID and timestamp of the written
    ///   message.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The <see cref="JsonLogWriter"/> has been disposed.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="log"/> is <see langword="null"/>.
    /// </exception>
    public async ValueTask<WriteResult> WriteToLogAsync<T>(IWriteAheadLog log, T data, JsonSerializerOptions? options = null) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(log);

        try {
            JsonSerializer.Serialize(_jsonWriter, data, options);
            return await _writer.WriteToLogAsync(log).ConfigureAwait(false);
        }
        finally {
            _jsonWriter.Reset();
        }
    }
    
    
    /// <summary>
    /// Writes the specified data to the log as a JSON-serialized message.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to write the JSON message to.
    /// </param>
    /// <param name="data">
    ///   The data to serialize to JSON and write to the log.
    /// </param>
    /// <param name="jsonTypeInfo">
    ///   The <see cref="JsonTypeInfo{T}"/> to use for serialization.
    /// </param>
    /// <typeparam name="T">
    ///   The type of the data to serialize to JSON.
    /// </typeparam>
    /// <returns>
    ///   A <see cref="WriteResult"/> containing the sequence ID and timestamp of the written
    ///   message.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The <see cref="JsonLogWriter"/> has been disposed.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="log"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="jsonTypeInfo"/> is <see langword="null"/>.
    /// </exception>
    public async ValueTask<WriteResult> WriteToLogAsync<T>(IWriteAheadLog log, T data, JsonTypeInfo<T> jsonTypeInfo) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);

        try {
            JsonSerializer.Serialize(_jsonWriter, data, jsonTypeInfo);
            return await _writer.WriteToLogAsync(log).ConfigureAwait(false);
        }
        finally {
            _jsonWriter.Reset();
        }
    }


    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed) {
            return;
        }
        
        _jsonWriter.Dispose();
        _writer.Dispose();
        
        _disposed = true;
    }


    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }
        
        await _jsonWriter.DisposeAsync().ConfigureAwait(false);
        await _writer.DisposeAsync().ConfigureAwait(false);
        
        _disposed = true;
    }

}
