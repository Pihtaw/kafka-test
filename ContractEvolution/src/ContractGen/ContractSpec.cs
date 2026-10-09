using System;
using System.Collections.Generic;
using System.Linq;

namespace ContractGen;

// Формат (по строке на версию, # — комментарий):
//   contract OrderCreated
//   v1 OrderId:string Amount:int        — исходный набор полей
//   v2 +Currency:string                 — добавили поле
//   v3 -Comment                         — удалили поле
//   v4 ~Quantity:long                   — поменяли тип поля

internal sealed class ContractSpec
{
    public string Name = "";
    public List<VersionSpec> Versions = new();

    public static readonly Dictionary<string, string> KnownTypes = new()
    {
        ["string"] = "string",
        ["string?"] = "string?",
        ["int"] = "int",
        ["long"] = "long",
        ["double"] = "double",
        ["decimal"] = "decimal",
        ["bool"] = "bool",
    };

    public static ContractSpec Parse(string text)
    {
        var spec = new ContractSpec();
        List<FieldSpec>? current = null;
        int lineNo = 0;

        foreach (var raw in text.Split('\n'))
        {
            lineNo++;
            var line = raw.Split('#')[0].Trim();
            if (line.Length == 0) continue;

            var parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts[0] == "contract")
            {
                if (parts.Length != 2) throw new SpecException(lineNo, "ожидалось: contract <Имя>");
                spec.Name = parts[1];
                continue;
            }

            if (parts[0].Length < 2 || parts[0][0] != 'v' || !int.TryParse(parts[0].Substring(1), out var number))
                throw new SpecException(lineNo, $"строка должна начинаться с vN, а начинается с '{parts[0]}'");
            if (number != spec.Versions.Count + 1)
                throw new SpecException(lineNo, $"ожидалась версия v{spec.Versions.Count + 1}, а не v{number}");

            var fields = current is null ? new List<FieldSpec>() : current.Select(f => f).ToList();
            var changes = new List<string>();

            foreach (var token in parts.Skip(1))
            {
                if (current is null)
                {
                    fields.Add(Field(token, lineNo));
                    continue;
                }

                char op = token[0];
                var body = token.Substring(1);
                switch (op)
                {
                    case '+':
                    {
                        var f = Field(body, lineNo);
                        if (fields.Any(x => x.Name == f.Name)) throw new SpecException(lineNo, $"поле {f.Name} уже есть");
                        fields.Add(f);
                        changes.Add($"добавили поле {f.Name} ({f.Type})");
                        break;
                    }
                    case '-':
                    {
                        int i = fields.FindIndex(x => x.Name == body);
                        if (i < 0) throw new SpecException(lineNo, $"нельзя удалить {body}: такого поля нет");
                        fields.RemoveAt(i);
                        changes.Add($"удалили поле {body}");
                        break;
                    }
                    case '~':
                    {
                        var f = Field(body, lineNo);
                        int i = fields.FindIndex(x => x.Name == f.Name);
                        if (i < 0) throw new SpecException(lineNo, $"нельзя поменять тип {f.Name}: такого поля нет");
                        changes.Add($"{f.Name}: {fields[i].Type} → {f.Type}");
                        fields[i] = f; // место поля в классе сохраняем
                        break;
                    }
                    default:
                        throw new SpecException(lineNo, $"изменение должно начинаться с +, - или ~: '{token}'");
                }
            }

            if (fields.Count == 0) throw new SpecException(lineNo, "в версии не осталось полей");
            spec.Versions.Add(new VersionSpec(number, fields, current is null ? "исходная версия" : string.Join("; ", changes)));
            current = fields;
        }

        if (spec.Name.Length == 0) throw new SpecException(1, "нет строки contract <Имя>");
        if (spec.Versions.Count == 0) throw new SpecException(1, "нет ни одной версии");
        return spec;
    }

    private static FieldSpec Field(string token, int lineNo)
    {
        var kv = token.Split(':');
        if (kv.Length != 2 || kv[0].Length == 0) throw new SpecException(lineNo, $"поле пишется как Имя:тип, а не '{token}'");
        if (!KnownTypes.ContainsKey(kv[1]))
            throw new SpecException(lineNo, $"неизвестный тип '{kv[1]}', можно: {string.Join(", ", KnownTypes.Keys)}");
        return new FieldSpec(kv[0], kv[1]);
    }
}

internal sealed record FieldSpec(string Name, string Type);

internal sealed record VersionSpec(int Number, List<FieldSpec> Fields, string Change);

internal sealed class SpecException(int line, string message) : Exception(message)
{
    public int Line { get; } = line;
}
