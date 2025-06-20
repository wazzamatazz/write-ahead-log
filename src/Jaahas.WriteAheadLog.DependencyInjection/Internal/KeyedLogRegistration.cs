namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

/// <summary>
/// Used to track named log registrations.
/// </summary>
/// <param name="Key">
///   The dependency injection key for the log.
/// </param>
internal record KeyedLogRegistration(string Key);
