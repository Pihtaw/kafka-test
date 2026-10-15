using System.Text;

namespace MessageCheck;

public static class HeaderNames
{
    public const string EventType = "Event-Type";
    public const string PackageVersion = "Event-PackageVersion";
}

public static class HeaderReader
{
    // на битых байтах кидаем искулючение
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static bool TryRead(IReadOnlyList<Header> headers, string name, Reason missingReason,
                               out string value, out Decision? fail)
    {
        value = "";
        fail = null;

        var found = headers.Where(h => h.Key == name).ToList();
        if (found.Count == 0)
        {
            fail = new Decision(Verdict.Rejected, missingReason, $"нет заголовка {name}");
            return false;
        }

        // теперь повтор заголовка — всегда отказ, даже если значения одинаковые:
        if (found.Count > 1)
        {
            fail = new Decision(Verdict.Rejected, Reason.DuplicateHeader, $"заголовок {name} встречается {found.Count} раза");
            return false;
        }

        // проверка на пустоту и UTF-8
        var values = new List<string>();
        foreach (var h in found)
        {
            if (h.Value is null || h.Value.Length == 0)
            {
                fail = new Decision(Verdict.Rejected, Reason.EmptyHeader, $"заголовок {name} пустой");
                return false;
            }

            string text;
            try { text = StrictUtf8.GetString(h.Value); }
            catch (DecoderFallbackException)
            {
                fail = new Decision(Verdict.Rejected, Reason.InvalidHeaderEncoding,
                    $"заголовок {name} не UTF-8: {Convert.ToHexString(h.Value)}");
                return false;
            }

            if (string.IsNullOrWhiteSpace(text))
            {
                fail = new Decision(Verdict.Rejected, Reason.EmptyHeader, $"заголовок {name} из одних пробелов");
                return false;
            }
            values.Add(text);
        }

        value = values[0];
        return true;
    }


    // необязательный заголовок
    // в случае отсутствия - не ошибка
    // в случае присутствия те же требования и проверки
    public static bool TryReadOptional(IReadOnlyList<Header> headers, string name,
                                       out string? value, out Decision? fail)
    {
        value = null;
        fail = null;
        if (!headers.Any(h => h.Key == name))
            return true;

        if (!TryRead(headers, name, Reason.EmptyHeader, out var text, out fail))
            return false;
        value = text;
        return true;
    }
}
