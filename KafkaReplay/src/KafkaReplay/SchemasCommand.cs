using System.Text.Encodings.Web;
using System.Text.Json;
using MessageCheck;

namespace KafkaReplay;

// replay schemas: JSON-схема каждого события из сборки контрактов — файл <полное имя типа>.json.
// Схемы строятся из тех же классов, которыми check читает события (см. SchemaGenerator), и дальше
// используются в check --schemas. Папку по умолчанию (contracts/schemas) git не видит: схемы компании в репозиторий не попадают
public static class SchemasCommand
{
    public const string DefaultOut = "contracts/schemas";

    // метка в "$comment": по ней отличаем наши сгенерированные схемы от официальных, положенных руками
    public const string GeneratedMark = "сгенерировано replay schemas";

    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public static int Run(string contractsPath, string outDir)
    {
        if (!ContractsLoader.TryLoad(contractsPath, out var contracts, out var error)) return Cli.Fail(error!);

        Directory.CreateDirectory(outDir);
        string source = $"{GeneratedMark} из {Path.GetFileName(contractsPath)} {contracts!.RegistryVersion}";
        var fresh = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (eventType, dtoType) in contracts.Catalog.Types.OrderBy(t => t.Key, StringComparer.Ordinal))
        {
            var schema = SchemaGenerator.Generate(dtoType);
            schema["$comment"] = source;
            string file = Path.GetFullPath(Path.Combine(outDir, eventType + ".json"));
            AtomicFile.WriteAllText(file, schema.ToJsonString(Options));
            fresh.Add(file);
        }

        // типы, которых больше нет в реестре: их старые схемы иначе продолжали бы использоваться в check --schemas.
        // Удаляем только свои (с меткой), официальные схемы без метки не трогаем
        var stale = Directory.EnumerateFiles(outDir, "*.json")
            .Select(Path.GetFullPath)
            .Where(f => !fresh.Contains(f) && File.ReadAllText(f).Contains(GeneratedMark, StringComparison.Ordinal))
            .ToList();
        stale.ForEach(File.Delete);

        Console.WriteLine($"Контракты: {contracts.Describe()}");
        Console.WriteLine($"JSON-схемы: {fresh.Count} файлов в {Path.GetFullPath(outDir)}" + (stale.Count > 0 ? $", удалено устаревших: {stale.Count}" : ""));
        Console.WriteLine($"Проверка по ним: replay check … --contracts {contractsPath} --schemas {outDir}");
        return ExitCodes.Ok;
    }
}
