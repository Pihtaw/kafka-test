# kafka-test

# День 2
| Папка / файл | Что это |
|---|---|
| `docker-compose.yml` | Kafka (KRaft, без ZooKeeper) и веб-интерфейсы для просмотра; утилиты для сравнения — в профиле `tools` |
| `Producer/` | Отправляет 10 сообщений в `test-topic`: ключ `order-0..2`, JSON, заголовки. `Acks.All`, батчинг `LingerMs = 5` |
| `Consumer/` | Читает `test-topic` в группе `test-group` (`Subscribe`). At least once: `StoreOffset` после обработки + автокоммит пачкой |
| `CrashTest/` | Эксперимент «упали посреди обработки»: режим `after` (сообщение перечитается) и `before` (сообщение потеряется) |
| `RangeReader/` | Читает диапазон offsets одной партиции через `Assign`, без коммита, с проверкой границ low/high; сверяет offset группы до и после |


## Установим

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (запущен, статус Engine running);
- [.NET SDK 8](https://dotnet.microsoft.com/download).

```bash
docker --version
dotnet --version
```

### 1. Как поднять Kafka

```bash
docker compose up -d
docker ps          # должны быть контейнеры kafka, kafka-ui, kafka-ui-provectus со статусом Up
```

Создать топик на 3 партиции:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-topic --partitions 3
```

Веб-интерфейсы:

- Kafbat UI - http://localhost:8080
- Provectus Kafka UI - http://localhost:8081

### 2. Отправить сообщения - Producer

```bash
cd Producer
dotnet run
```

### 3. Прочитать сообщения - Consumer

```bash
cd Consumer
dotnet run
```

### 4. Прочитать диапазон без коммита - RangeReader

Перед запуском **остановить все консьюмеры**, (мы проверяем не будет ли сдвинут оффсет).

```bash
cd RangeReader
dotnet run                                  # по умолчанию: test-topic, партиция 0, offsets 2..5
dotnet run -- <топик> <партиция> <from> <to>
```

Offset группы равен 7, а не 6: это номер **следующего** сообщения, которое группа прочитает.

Проверка границ - программа должна отказать с причиной:

| Команда | Ожидаемый результат |
|---|---|
| `dotnet run -- test-topic 0 0 1000` | offset 1000 ещё не существует (правая граница) |
| `dotnet run -- test-topic 2 0 0` | партиция пустая |
| `dotnet run -- test-topic 0 5 2` | начало диапазона больше конца |

Независимая проверка offset группы средствами с помощью только Kafka:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group test-group
```

### 5. Эксперимент с падением - CrashTest

Программа падает на сообщении с `orderId 5`, пока рядом нет файла `fixed.txt` («баг починили»).

```bash
cd CrashTest
rm -f fixed.txt

# коммит ПОСЛЕ обработки (at least once)
dotnet run -- after    # падает на orderId 5
dotnet run -- after    # снова падает на том же сообщении - оно не закоммичено
touch fixed.txt
dotnet run -- after    # orderId 5 обработан, ничего не потеряно

# коммит ДО обработки
rm -f fixed.txt
dotnet run -- before   # падает на orderId 5
touch fixed.txt
dotnet run -- before   # orderId 5 больше не появится - сообщение потеряно
```

Важно: между `dotnet run` и `after`/`before` должен быть пробел после `--`.
Каждый режим использует свою группу (`crash-test-after-v2`, `crash-test-before-v2`); чтобы повторить эксперимент с нуля, поменяйте суффикс группы в коде или очистите Kafka (см. ниже).

## Проведенные эксперименты

**Два консьюмера в одной группе.** Запустить `Consumer` в двух терминалах, затем `Producer`.
3 партиции делятся между двумя консьюмерами как 2 + 1; второму может достаться пустая партиция 2 (см. таблицу хэшей выше).
Если остановить один консьюмер, через несколько секунд второй заберёт все партиции.

**Сообщения без ключа.** В `Producer/Program.cs` раскомментирован вариант конфига с `Partitioner.Consistent` и `Key = null!` -
при нём все сообщения без ключа попадают в одну партицию.
