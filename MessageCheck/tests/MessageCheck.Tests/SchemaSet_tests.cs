using MessageCheck;

namespace MessageCheck.Tests;

// схемы из папки: поиск по title / имени файла / имени класса
public class SchemaSetTests
{
    private static readonly string Folder = Path.Combine(AppContext.BaseDirectory, "Fixtures", "schemas");

    private static SchemaSet Load()
    {
        Assert.True(SchemaSet.TryLoad(Folder, out var schemas, out var error), error);
        return schemas!;
    }

    [Fact]
    public void Schema_is_found_by_title_and_by_file_name()
    {
        var schemas = Load();
        Assert.Equal(2, schemas.Count);
        Assert.Equal("OrderCreated", schemas.Find("OrderCreated")!.Name);           // title
        Assert.Equal("OrderCancelled", schemas.Find("OrderCancelled")!.Name);       // имя файла
    }

    [Fact]
    public void Full_event_type_falls_back_to_class_name()
    {
        var schemas = Load();
        Assert.NotNull(schemas.Find("MessageCheck.SampleContracts.OrderCreated"));
        Assert.Null(schemas.Find("MessageCheck.SampleContracts.Unknown"));
        Assert.Null(schemas.Check("MessageCheck.SampleContracts.Unknown", "{}"u8.ToArray()));
    }

    [Fact]
    public void Check_reports_violations_of_found_schema()
    {
        var v = Assert.Single(Load().Check("MessageCheck.SampleContracts.OrderCreated", """{"OrderId":1,"Amount":500}"""u8.ToArray())!);
        Assert.Equal("$.Currency", v.Path);
        Assert.Equal("required-null", v.Rule);   // Currency может быть null, отсутствие поля для контракта то же, что null
        Assert.False(v.IsWarning);
    }

    [Fact]
    public void Unsupported_keywords_are_shown_in_description() =>
        Assert.Contains("minLength", Load().Describe(), StringComparison.Ordinal);

    [Fact]
    public void Missing_folder_is_an_error()
    {
        Assert.False(SchemaSet.TryLoad(Path.Combine(Folder, "nope"), out var schemas, out var error));
        Assert.Null(schemas);
        Assert.Contains("нет папки", error!, StringComparison.Ordinal);
    }

    [Fact]
    public void Event_type_case_matters_as_in_dodo_kafka()
    {
        var schemas = Load();
        Assert.Null(schemas.Find("ordercreated"));
        Assert.Null(schemas.Find("MessageCheck.SampleContracts.ORDERCREATED"));
    }
}
