# Совместимость версий OrderCreated

Источник событий: прочитаны из Kafka, топик order-created-evolution-1791441435. Вопрос: может ли сервис со СТАРОЙ версией контракта прочитать событие, отправленное НОВОЙ версией.

## Версии

| версия | что поменялось |
|---|---|
| v1 | исходная версия |
| v2 | добавили поле Currency (string) |
| v3 | удалили поле Comment |
| v4 | Quantity: int → long |
| v5 | Amount: int → double |
| v6 | OrderId: string → int |

## Потребитель: System.Text.Json, Web-настройки (лишние поля игнорируются)

| отправил ↓ / читает → | v1 | v2 | v3 | v4 | v5 | v6 |
|---|---|---|---|---|---|---|
| **v1** | ✅ |  |  |  |  |  |
| **v2** | новые поля не видим 13/13 | ✅ |  |  |  |  |
| **v3** | данные не те 11/11 | данные не те 11/11 | ✅ |  |  |  |
| **v4** | не ок 3/11 | не ок 3/11 | не ок 3/11 | ✅ |  |  |
| **v5** | не ок 5/11 | не ок 5/11 | не ок 5/11 | не ок 2/11 | ✅ |  |
| **v6** | не ок 12/12 | не ок 12/12 | не ок 12/12 | не ок 12/12 | не ок 12/12 | ✅ |

все события прочитались без потерь · прочитались, новые поля старый контракт просто не видит · прочитались БЕЗ ошибки, но данные не те: поле не пришло и стало default или значение изменилось · десериализация упала. Число — сколько событий из скольких с этим исходом.

### Что пошло не так

| отправил → читает | исход | подробности | на каких событиях |
|---|---|---|---|
| v3 → v1 | данные не те SilentDefault | comment не пришло → ""; currency не виден | все 11 |
| v3 → v2 | данные не те SilentDefault | comment не пришло → "" | все 11 |
| v4 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v4 → v1 | данные не те SilentDefault | comment не пришло → ""; currency не виден | обычное, OrderId = "", OrderId = null, Amount = 0, Amount = int.MaxValue, Amount = int.MinValue, Currency = "", Currency = null |
| v4 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v4 → v2 | данные не те SilentDefault | comment не пришло → "" | обычное, OrderId = "", OrderId = null, Amount = 0, Amount = int.MaxValue, Amount = int.MinValue, Currency = "", Currency = null |
| v4 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v1 | данные не те SilentDefault | comment не пришло → ""; currency не виден | обычное, OrderId = "", OrderId = null, Amount = 2.0, Currency = "", Currency = null |
| v5 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v2 | данные не те SilentDefault | comment не пришло → "" | обычное, OrderId = "", OrderId = null, Amount = 2.0, Currency = "", Currency = null |
| v5 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v4 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v6 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v4 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v5 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |

## Потребитель: MessageCheck.JsonModule (лишние поля запрещены)

| отправил ↓ / читает → | v1 | v2 | v3 | v4 | v5 | v6 |
|---|---|---|---|---|---|---|
| **v1** | ✅ |  |  |  |  |  |
| **v2** | не ок 13/13 | ✅ |  |  |  |  |
| **v3** | не ок 11/11 | данные не те 11/11 | ✅ |  |  |  |
| **v4** | не ок 11/11 | не ок 3/11 | не ок 3/11 | ✅ |  |  |
| **v5** | не ок 11/11 | не ок 5/11 | не ок 5/11 | не ок 2/11 | ✅ |  |
| **v6** | не ок 12/12 | не ок 12/12 | не ок 12/12 | не ок 12/12 | не ок 12/12 | ✅ |

все события прочитались без потерь · прочитались, новые поля старый контракт просто не видит · прочитались БЕЗ ошибки, но данные не те: поле не пришло и стало default или значение изменилось · десериализация упала. Число — сколько событий из скольких с этим исходом.

### Что пошло не так

| отправил → читает | исход | подробности | на каких событиях |
|---|---|---|---|
| v2 → v1 | не ок Exploded | JsonException: The JSON property 'currency' could not be mapped to any .NET member contained in type 'Contracts.V1.OrderCreated'. | все 13 |
| v3 → v1 | не ок Exploded | JsonException: The JSON property 'currency' could not be mapped to any .NET member contained in type 'Contracts.V1.OrderCreated'. | все 11 |
| v3 → v2 | данные не те SilentDefault | comment не пришло → "" | все 11 |
| v4 → v1 | не ок Exploded | JsonException: The JSON property 'currency' could not be mapped to any .NET member contained in type 'Contracts.V1.OrderCreated'. | обычное, OrderId = "", OrderId = null, Amount = 0, Amount = int.MaxValue, Amount = int.MinValue, Currency = "", Currency = null |
| v4 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v4 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v4 → v2 | данные не те SilentDefault | comment не пришло → "" | обычное, OrderId = "", OrderId = null, Amount = 0, Amount = int.MaxValue, Amount = int.MinValue, Currency = "", Currency = null |
| v4 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v1 | не ок Exploded | JsonException: The JSON property 'currency' could not be mapped to any .NET member contained in type 'Contracts.V1.OrderCreated'. | обычное, OrderId = "", OrderId = null, Amount = 2.0, Currency = "", Currency = null |
| v5 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v2 | данные не те SilentDefault | comment не пришло → "" | обычное, OrderId = "", OrderId = null, Amount = 2.0, Currency = "", Currency = null |
| v5 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v5 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.quantity | Quantity = int.MaxValue + 1, Quantity = int.MinValue − 1, Quantity = long.MaxValue |
| v5 → v4 | не ок Exploded | JsonException: The JSON value could not be converted to System.Int32. Path: $.amount | Amount = 1.5, Amount = 1e20 |
| v6 → v1 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v2 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v3 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v4 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |
| v6 → v5 | не ок Exploded | JsonException: The JSON value could not be converted to System.String. Path: $.orderId | все 12 |

