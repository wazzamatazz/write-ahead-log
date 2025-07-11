using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// Helper methods for throwing exceptions when circumstances are met.
/// </summary>
internal static class ExceptionHelper {
    
    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> if the specified <paramref name="argument"/> is
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="argument">
    ///   The reference type argument to validate as non-<see langword="null"/>.
    /// </param>
    /// <param name="paramName">
    ///   The name of the parameter with which <paramref name="argument"/> corresponds. If you omit
    ///   this parameter, the name of argument is used.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="argument"/> is <see langword="null"/>.
    /// </exception>
    public static void ThrowIfNull(object? argument, [CallerArgumentExpression(nameof(argument))] string? paramName = null) {
#if NETCOREAPP
        ArgumentNullException.ThrowIfNull(argument, paramName);
#else
        if (argument is null){
            throw new ArgumentNullException(paramName);
        }  
#endif
    }
    
    
    /// <summary>
    /// Throws an exception if <paramref name="argument"/> is <see langword="null"/>, empty, or
    /// consists only of white-space characters.
    /// </summary>
    /// <param name="argument">
    ///   The string argument to validate.
    /// </param>
    /// <param name="paramName">
    ///   The name of the parameter with which <paramref name="argument"/> corresponds. If you omit
    ///   this parameter, the name of argument is used.
    /// </param>
    /// <exception cref="ArgumentException">
    ///   <paramref name="argument"/> is <see langword="null"/>, empty of consists only of
    ///   white-space characters.
    /// </exception>
    public static void ThrowIfNullOrWhiteSpace(string? argument, [CallerArgumentExpression(nameof(argument))] string? paramName = null) {
#if NETCOREAPP
        ArgumentException.ThrowIfNullOrWhiteSpace(argument, paramName);
#else
        if (string.IsNullOrWhiteSpace(argument)){
            throw new ArgumentException("The value cannot be an empty string or composed entirely of whitespace.", paramName);
        }  
#endif
    }
    
    
    /// <summary>
    /// Throws an <see cref="ObjectDisposedException"/> if the specified <paramref name="condition"/>
    /// is <see langword="true"/>.
    /// </summary>
    /// <param name="condition">
    ///   The condition to evaluate.
    /// </param>
    /// <param name="instance">
    ///   The object whose type's full name should be included in any resulting <see cref="ObjectDisposedException"/>.
    /// </param>
#if NETCOREAPP
    [System.Diagnostics.StackTraceHidden]
#endif
    public static void ThrowIfDisposed([DoesNotReturnIf(true)] bool condition, object instance) {
#if NETCOREAPP
        ObjectDisposedException.ThrowIf(condition, instance);
#else
        if (condition) {
            throw new ObjectDisposedException(instance.GetType().FullName);
        } 
#endif
    }

}
