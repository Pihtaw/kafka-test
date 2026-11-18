using KafkaReplay;

namespace KafkaReplay.Tests;

public class RoutesTests
{
    private static Routes Parse(string json)
    {
        Assert.True(Routes.TryParse(json, out var r, out var error), error);
        return r!;
    }

    [Fact]
    public void Event_type_is_routed_to_its_topic()
    {
        var r = Parse("""{"A.OrderCreated":"orders-copy","A.OrderCancelled":"cancel-copy"}""");
        Assert.Equal("orders-copy", r.TargetFor("A.OrderCreated"));
        Assert.Equal("cancel-copy", r.TargetFor("A.OrderCancelled"));
    }

    [Fact]
    public void Unknown_type_has_no_route() =>
        Assert.Null(Parse("""{"A.OrderCreated":"orders-copy"}""").TargetFor("A.Other"));

    [Fact]
    public void Star_is_default_route() =>
        Assert.Equal("rest", Parse("""{"A.OrderCreated":"orders-copy","*":"rest"}""").TargetFor("A.Other"));

    [Fact]
    public void Empty_table_is_rejected()
    {
        Assert.False(Routes.TryParse("{}", out _, out var error));
        Assert.Contains("пустая", error);
    }

    [Fact]
    public void Not_an_object_is_rejected()
    {
        Assert.False(Routes.TryParse("""["orders-copy"]""", out _, out var error));
        Assert.Contains("JSON-объект", error);
    }

    [Fact]
    public void Route_back_to_source_on_same_cluster_is_found()
    {
        var r = Parse("""{"A.OrderCreated":"orders","A.OrderCancelled":"orders-copy"}""");
        var back = r.RoutesBackToSource("orders", "localhost:9092", "LOCALHOST:9092");
        Assert.Equal(1, back.Count);
        Assert.Contains("A.OrderCreated", back[0]);
    }

    [Fact]
    public void Same_topic_name_on_other_cluster_is_allowed() =>
        Assert.Equal(0, Parse("""{"A.OrderCreated":"orders"}""").RoutesBackToSource("orders", "kafka-a:9092", "ns.servicebus.windows.net:9093").Count);

    [Fact]
    public void Bootstrap_order_does_not_matter() =>
        Assert.True(Routes.SameCluster("a:1, b:2", "b:2,a:1"));

    [Fact]
    public void Event_type_is_case_sensitive_as_in_dodo_kafka()
    {
        var r = Parse("""{"A.OrderCreated":"orders-copy"}""");
        Assert.Null(r.TargetFor("a.ordercreated"));
        Assert.False(r.HasRoute("a.ordercreated"));
    }

    [Fact]
    public void Null_means_do_not_send_explicitly()
    {
        var r = Parse("""{"A.OrderCreated":"orders-copy","A.OrderPaid":null}""");
        Assert.Null(r.TargetFor("A.OrderPaid"));
        Assert.True(r.HasRoute("A.OrderPaid"));      // решение есть — план строится
        Assert.False(r.HasRoute("A.Other"));         // решения нет — план не строится
        Assert.Equal(new[] { "orders-copy" }, r.TargetTopics);
    }

    [Fact]
    public void Star_null_skips_all_other_types()
    {
        var r = Parse("""{"A.OrderCreated":"orders-copy","*":null}""");
        Assert.True(r.HasRoute("A.Other"));
        Assert.Null(r.TargetFor("A.Other"));
    }

    [Fact]
    public void Empty_topic_is_still_rejected()
    {
        Assert.False(Routes.TryParse("""{"A.OrderCreated":" "}""", out _, out var error));
        Assert.Contains("пустой", error);
    }
}
