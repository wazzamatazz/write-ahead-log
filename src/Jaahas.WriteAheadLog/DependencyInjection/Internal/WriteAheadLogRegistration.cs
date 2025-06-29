namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

/// <summary>
/// A registration for a Write-Ahead Log (WAL) implementation.
/// </summary>
/// <param name="Name">
///   The name of the log.
/// </param>
/// <param name="Factory">
///   The factory function for creating an instance of the log.
/// </param>
internal record WriteAheadLogRegistration(string Name, Func<IServiceProvider, IWriteAheadLog> Factory); 
