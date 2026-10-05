using System.Text;

namespace MessageCheck;

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

        // повторы .net берет последнее, другой клиент не знаем - тогда отказ
        // повторы одинаковые - пропускаем
        var distinct = values.Distinct().ToList();
        if (distinct.Count > 1)
        {
            fail = new Decision(Verdict.Rejected, Reason.DuplicateHeader,
                $"заголовок {name} повторяется с разными значениями: {string.Join(", ", distinct)}");
            return false;
        }

        value = distinct[0];
        return true;
    }
}
