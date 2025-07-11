#if !NETCOREAPP
namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// .NET Standard 2.0 polyfills.
/// </summary>
internal static class CollectionExtensions {

    /// <summary>
    /// Gets the value associated with the specified key, or returns the default value if the key is not found.
    /// </summary>
    /// <param name="dictionary">
    ///   The dictionary to search for the key.
    /// </param>
    /// <param name="key">
    ///   The key.
    /// </param>
    /// <typeparam name="TKey">
    ///   The type of the keys in the dictionary.
    /// </typeparam>
    /// <typeparam name="TValue">
    ///   The type of the values in the dictionary.
    /// </typeparam>
    /// <returns>
    ///   The value associated with the specified key, or the default value for the type if the key is not found.
    /// </returns>
    public static TValue? GetValueOrDefault<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dictionary, TKey key) {
        return dictionary.TryGetValue(key, out var value) 
            ? value 
            : default;
    }

}
#endif
