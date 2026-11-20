using System.Globalization;
using KafkaReplay;

// фразы самого System.CommandLine (справка, «неизвестный параметр») — по-русски, как и наши
CultureInfo.CurrentUICulture = CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.GetCultureInfo("ru-RU");

// replay check … | replay execute … | replay --help
var cli = new ReplayCli(DateTimeOffset.Now);

// без аргументов — справка (и код 3: запуск ничего не сделал)
if (args.Length == 0)
{
    await cli.Root.Parse(["--help"]).InvokeAsync().ConfigureAwait(false);
    return ExitCodes.Blocked;
}

var parsed = cli.Root.Parse(args);

// ошибки разбора — наш код выхода 3 и подсказка про --help (System.CommandLine по умолчанию вернул бы 1)
if (parsed.Errors.Count > 0)
    return Cli.Fail(ReplayCli.Describe(parsed.Errors));

return await parsed.InvokeAsync().ConfigureAwait(false);
