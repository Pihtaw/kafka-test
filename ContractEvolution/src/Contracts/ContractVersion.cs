namespace Contracts;

// одна версия: номер, сгенерированный тип и что поменялось относительно предыдущей
public sealed record ContractVersion(int Version, Type Type, string Change);
