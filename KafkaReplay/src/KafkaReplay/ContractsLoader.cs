using System.Collections;
using System.Reflection;
using System.Runtime.Loader;
using MessageCheck;

namespace KafkaReplay;

// Контракты, загруженные из собранного реестра событий
public sealed record LoadedContracts(
    string RegistryPath,
    string RegistryVersion,
    EventCatalog Catalog,
    IReadOnlyDictionary<string, IReadOnlySet<string>> EventTypesByTopic,  // топик -> полные имена типов
    IReadOnlyDictionary<string, string> ContractVersions)                 // полное имя типа -> версия его сборки
{
    public string Describe() =>
        $"{Path.GetFileName(RegistryPath)} {RegistryVersion}: {Catalog.Count} типов событий в {EventTypesByTopic.Count} топиках";

    public bool HasTopic(string topic) => EventTypesByTopic.ContainsKey(topic);

    // объявлен ли тип в реестре для этого топика
    public bool IsDeclaredFor(string topic, string eventType) =>
        EventTypesByTopic.TryGetValue(topic, out var types) && types.Contains(eventType);

    public string? VersionOf(string eventType) => ContractVersions.GetValueOrDefault(eventType);
}

// Загрузка контрактов из сборки реестра (в event-registry это Dodo.EventRegistry.dll).
// Какие классы считать событиями, берём из реестра топиков
// в сборке ищется статический класс Topics, у которого публичные статические поля — топики
// со свойствами Name (имя топика) и EventTypes (типы событий в нём). Так же устроен реестр в event-registry.
public static class ContractsLoader
{
    public static bool TryLoad(string registryPath, out LoadedContracts? contracts, out string? error)
    {
        contracts = null;
        string fullPath = Path.GetFullPath(registryPath);
        if (!File.Exists(fullPath)) { error = $"нет файла {fullPath}"; return false; }

        Assembly registry;
        List<(string Topic, IEnumerable<Type> Types)> topics;
        try
        {
            var context = new ContractsLoadContext(Path.GetDirectoryName(fullPath)!);
            registry = context.LoadFromAssemblyPath(fullPath);
            topics = ReadTopics(registry);

            foreach (var (_, types) in topics)
                foreach (var type in types)
                    foreach (var property in type.GetProperties())
                        _ = property.PropertyType.FullName;
        }
        catch (BadImageFormatException)
        {
            error = $"{fullPath} — не сборка .NET";
            return false;
        }
        catch (Exception e) when (e is FileNotFoundException or FileLoadException or TypeLoadException
                                       or ReflectionTypeLoadException or TargetInvocationException or TypeInitializationException)
        {
            var root = e;
            while (root.InnerException is not null) root = root.InnerException;
            error = $"не удалось загрузить контракты из {fullPath}: {root.Message.Trim()} (утилита работает на .NET {Environment.Version})";
            if (root is FileNotFoundException)
                error += ". Если не хватает сборки из NuGet-пакета (например, Dodo.Primitives), собери реестр с -p:CopyLocalLockFileAssemblies=true";
            return false;
        }

        if (topics.Count == 0)
        {
            error = $"в {Path.GetFileName(fullPath)} нет класса Topics с топиками (статические поля со свойствами Name и EventTypes)";
            return false;
        }

        var catalog = new EventCatalog();
        var byTopic = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        var versions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var seen = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        foreach (var (topic, types) in topics)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var type in types)
            {
                string name = type.FullName!;
                if (seen.TryGetValue(name, out var other) && other != type)
                {
                    error = $"тип {name} объявлен в двух разных сборках: {other.Assembly.GetName().Name} и {type.Assembly.GetName().Name}";
                    return false;
                }
                seen[name] = type;
                catalog.Add(type);
                versions[name] = VersionOf(type.Assembly);
                names.Add(name);
            }
            byTopic[topic] = names;
        }

        contracts = new LoadedContracts(fullPath, VersionOf(registry), catalog, byTopic, versions);
        error = null;
        return true;
    }

    private static List<(string, IEnumerable<Type>)> ReadTopics(Assembly registry)
    {
        var result = new List<(string, IEnumerable<Type>)>();
        foreach (var holder in registry.GetExportedTypes().Where(t => t.Name == "Topics" && t.IsAbstract && t.IsSealed))
        {
            foreach (var field in holder.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (field.GetValue(null) is not { } topic) continue;
                var t = topic.GetType();
                if (t.GetProperty("Name")?.GetValue(topic) is not string name) continue;
                if (t.GetProperty("EventTypes")?.GetValue(topic) is not IEnumerable eventTypes) continue;
                result.Add((name, eventTypes.OfType<Type>().ToList()));
            }
        }
        return result;
    }

    private static string VersionOf(Assembly assembly)
    {
        var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(informational)) return informational.Split('+')[0];
        return assembly.GetName().Version?.ToString() ?? "?";
    }

    private sealed class ContractsLoadContext(string directory) : AssemblyLoadContext("kafka-replay-contracts")
    {
        protected override Assembly? Load(AssemblyName name)
        {
            string n = name.Name ?? "";
            if (n is "System" or "mscorlib" or "netstandard" || n.StartsWith("System.") || n.StartsWith("Microsoft."))
                return null; // null — взять из общего контекста

            string path = Path.Combine(directory, n + ".dll");
            return File.Exists(path) ? LoadFromAssemblyPath(path) : null;
        }
    }
}
