using KafkaReplay;

if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine(CliOptions.Usage);
    return args.Length == 0 ? ExitCodes.Blocked : ExitCodes.Ok;
}

if (!CliOptions.TryParse(args, DateTimeOffset.Now, out var opt, out var error))
    return Cli.Fail(error!);

return opt!.Mode == RunMode.Execute
    ? await ExecuteCommand.RunAsync(opt).ConfigureAwait(false)
    : await DryRunCommand.RunAsync(opt).ConfigureAwait(false);
