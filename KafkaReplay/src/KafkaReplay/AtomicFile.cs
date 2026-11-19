namespace KafkaReplay;

// Запись файла «целиком или никак»: сначала во временный файл рядом (path.tmp), потом переименование поверх.
// Переименование внутри одной папки атомарно (rename в Linux/macOS, замена через MoveFileEx в Windows):
// если процесс прервут в любой момент, на диске будет либо старый файл целиком, либо новый целиком, но не половина.
// Брошенный .tmp от прерванной записи безвреден: следующая запись его перезапишет
public static class AtomicFile
{
    public static void WriteAllText(string path, string content)
    {
        string tmp = path + ".tmp";
        File.WriteAllText(tmp, content);
        File.Move(tmp, path, overwrite: true);
    }

    public static async Task WriteAllTextAsync(string path, string content)
    {
        string tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, content).ConfigureAwait(false);
        File.Move(tmp, path, overwrite: true);
    }
}
