using Confluent.Kafka;
using KafkaReplay;

namespace KafkaReplay.Tests;

// Строки подключения в формате kafka-tools
public class ConnectionSettingsTests
{
    private static ConnectionSettings Parse(string connectionString)
    {
        Assert.True(ConnectionSettings.TryParse(connectionString, out var s, out var error), error);
        return s!;
    }

    private static string ParseError(string connectionString)
    {
        Assert.False(ConnectionSettings.TryParse(connectionString, out var s, out var error));
        Assert.Null(s);
        return error!;
    }

    [Fact]
    public void Dev_stand_without_type_is_plain()
    {
        var s = Parse("BootstrapServers=kafka.stand-name:9092;");
        var c = s.ToClientConfig();

        Assert.Equal("plain", s.Type);
        Assert.Equal("kafka.stand-name:9092", c.BootstrapServers);
        Assert.Null(c.SecurityProtocol);
        Assert.Null(c.SaslPassword);
    }

    [Fact]
    public void Eventhub_sas_builds_event_hubs_connection_string()
    {
        var c = Parse("BootstrapServers=ns.servicebus.windows.net:9093;Type=EventHubSas;SharedAccessKeyName=listen;SharedAccessKey=abc+def=").ToClientConfig();

        Assert.Equal("ns.servicebus.windows.net:9093", c.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslSsl, c.SecurityProtocol);
        Assert.Equal(SaslMechanism.Plain, c.SaslMechanism);
        Assert.Equal("$ConnectionString", c.SaslUsername);
        // '=' в конце ключа не потерялся: делим по первому '='
        Assert.Equal("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=abc+def=", c.SaslPassword);
    }

    [Fact]
    public void Yandex_uses_scram_without_certificate_check()
    {
        var c = Parse("BootstrapServers=c-id.mdb.yandexcloud.net:9091;Type=Yandex;Username=user;Password=pass").ToClientConfig();

        Assert.Equal(SecurityProtocol.SaslSsl, c.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, c.SaslMechanism);
        Assert.Equal("user", c.SaslUsername);
        Assert.Equal("pass", c.SaslPassword);
        Assert.False(c.EnableSslCertificateVerification!.Value);
    }

    [Fact]
    public void Common_parameters_are_set_for_every_type()
    {
        foreach (var cs in new[]
        {
            "BootstrapServers=localhost:9092",
            "BootstrapServers=ns.servicebus.windows.net:9093;Type=EventHubSas;SharedAccessKeyName=n;SharedAccessKey=k",
            "BootstrapServers=c.mdb.yandexcloud.net:9091;Type=Yandex;Username=u;Password=p"
        })
        {
            var c = Parse(cs).ToClientConfig();
            Assert.True(c.SocketKeepaliveEnable);
            Assert.Equal(180_000, c.MetadataMaxAgeMs);
            Assert.Equal("kafka-replay", c.ClientId);
        }
    }

    [Fact]
    public void Names_and_type_are_case_insensitive()
    {
        var s = Parse("bootstrapservers=h:9091;TYPE=yandex;username=u;PASSWORD=p");
        Assert.Equal("yandex", s.Type);
        Assert.Equal("h:9091", s.BootstrapServers);
    }

    [Fact]
    public void Plain_from_bootstrap_matches_dev_stand_string()
    {
        var a = ConnectionSettings.Plain("localhost:9092").ToClientConfig();
        var b = Parse("BootstrapServers=localhost:9092;").ToClientConfig();
        Assert.Equal(Flat(b), Flat(a));

        static string Flat(ClientConfig c) => string.Join(";", c.OrderBy(x => x.Key).Select(x => $"{x.Key}={x.Value}"));
    }

    [Fact]
    public void Bootstrap_is_required() =>
        Assert.Contains("BootstrapServers", ParseError("Type=EventHubSas;SharedAccessKeyName=n;SharedAccessKey=k"));

    [Fact]
    public void Missing_field_is_named()
    {
        var error = ParseError("BootstrapServers=h:9091;Type=Yandex;Username=u");
        Assert.Contains("Password", error);
    }

    [Fact]
    public void Secret_is_never_in_error()
    {
        var error = ParseError("BootstrapServers=h:9091;Type=Yandex;Password=hunter2;Password=hunter3");
        Assert.DoesNotContain("hunter", error);
    }

    [Fact]
    public void Unknown_type_is_rejected() =>
        Assert.Contains("Kerberos", ParseError("BootstrapServers=h:1;Type=Kerberos"));

    [Fact]
    public void Eventhub_ad_is_not_supported_yet() =>
        Assert.Contains("пока не поддерживается", ParseError("BootstrapServers=ns.servicebus.windows.net:9093;Type=EventHubAd;"));

    [Fact]
    public void Part_without_equals_is_rejected() =>
        Assert.Contains("Имя=значение", ParseError("BootstrapServers=h:1;garbage"));

    [Fact]
    public void Describe_does_not_show_secrets()
    {
        var s = Parse("BootstrapServers=ns.servicebus.windows.net:9093;Type=EventHubSas;SharedAccessKeyName=listen;SharedAccessKey=secret");
        Assert.Equal("eventhubsas ns.servicebus.windows.net:9093", s.Describe());
    }
}
