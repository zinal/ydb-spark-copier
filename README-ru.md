# YDB Copy Table Sample

Пример Java-приложения для экспорта и импорта таблиц YDB с использованием Apache Spark и YDB Spark Connector. Подключается к YDB через Catalog API и использует Parquet в качестве промежуточного формата хранения.

## Принципы работы

Приложение работает в двух режимах:

1. **Export** — читает данные из таблицы YDB через Spark SQL, при необходимости применяя фильтр `WHERE`, и записывает результат в каталог Parquet на локальной файловой системе.
2. **Import** — читает данные из каталога Parquet, при необходимости применяет фильтр `WHERE` и вставляет результат в таблицу YDB.

### Как это устроено

- **Интеграция с каталогом**: YDB представлен как каталог Spark SQL с именем `src`. Конфигурация каталога (URL подключения, аутентификация и т.д.) загружается из XML-файла свойств.
- **Экспорт**: `SELECT * FROM src.<table> [WHERE ...]` → Spark DataFrame → файлы Parquet (режим перезаписи).
- **Импорт**: каталог Parquet → Spark DataFrame → временное представление → `SELECT * FROM parquet_src [WHERE ...]` → таблица YDB.
- **Именование таблиц**: пути к таблицам YDB указываются нативно со слэшами в качестве разделителей схемы, но без имени базы данных (например, `table_in_root` или `schema/table_in_schema`). Слэши в имени таблицы автоматически преобразуются в точки для совместимости с нотацией Spark SQL при необходимости.

## Требования

- Java 17
- Maven 3.6+
- Экземпляр YDB (локальный Docker или удалённый)

## Сборка

```bash
mvn clean package
```

В результате создаётся `target/ydb-copier-1.0-SNAPSHOT.jar` и, при использовании release-профиля, `target/ydb-copier-1.0-SNAPSHOT-bin.zip` с JAR, зависимостями и скриптами.

## Конфигурация

Отредактируйте `spark-config.xml` (или ваш конфигурационный файл) для задания параметров подключения к YDB:

- **spark.sql.catalog.src** — класс реализации каталога YDB
- **spark.sql.catalog.src.url** — строка подключения к YDB (например, `grpc://localhost:2136/local` для локального Docker)
- **spark.sql.catalog.src.auth.token.file** — путь к файлу токена для облачного/удалённого YDB (раскомментируйте при необходимости)
- **spark.sql.catalog.src.auth.ca.file** — путь к TLS-сертификату для защищённых соединений (раскомментируйте при необходимости)

Подробнее см. [документацию YDB Spark](https://ydb.tech/docs/ru/integrations/ingestion/spark).

### Ядра и память

- **spark.executor.cores** — задаёт количество рабочих потоков на исполнитель. Настраивается в `spark-config.xml` или через опцию `--conf` при использовании `spark-submit`.
- **spark.executor.memory** — должно быть не менее 1G на ядро (например, 4G для 4 ядер). Задаётся в `spark-config.xml` или через `--conf`.
- **-Xmx** (куча JVM) — при запуске через стартовый скрипт (например, `ydb-copier.sh`) задайте `-Xmx` примерно в два раза больше значения `spark.executor.memory`. Например, при `spark.executor.memory=4g` используйте `-Xmx8g` или `-Xmx8192m`.

## Использование

### Параметры командной строки

| Параметр | Описание |
|----------|----------|
| `--config <path>` | Путь к XML-конфигу Spark (по умолчанию: `spark-config.xml` в текущей директории) |
| `--mode export\|import` | Режим выполнения (обязательный) |
| `--table <name>` | Путь к таблице YDB (например, `mydb.myschema.mytable`) |
| `--directory <path>` | Локальная директория: выходная для экспорта, входная для импорта |
| `--filter <sql>` | Необязательное условие WHERE (например, `id>100 AND status='active'`) |

### Примеры

**Экспорт таблицы YDB в Parquet:**
```bash
./ydb-copier.sh --config spark-config.xml --mode export --table myschema/mytable --directory /tmp/parquet-out
```

**Экспорт с фильтром:**
```bash
./ydb-copier.sh --config spark-config.xml --mode export --table myschema/mytable --directory /tmp/parquet-out --filter "id > 100 AND status='active'"
```

**Импорт Parquet в YDB:**
```bash
./ydb-copier.sh --config spark-config.xml --mode import --table myschema/mytable --directory /tmp/parquet-in
```

**Импорт с фильтром:**
```bash
./ydb-copier.sh --config spark-config.xml --mode import --table myschema/mytable --directory /tmp/parquet-in --filter "dt <= '2026-02-03T00:00:00.000000Z'"
```

### spark-submit

```bash
spark-submit \
  --master "local[*]" \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.1.0 \
  --conf spark.executor.memory=4g \
  --class copytab.Copier \
  target/ydb-copier-1.0-SNAPSHOT.jar \
  --config spark-config.xml --mode export --table mydb.mytable --directory /tmp/out
```

При использовании собранного zip-архива запускайте из распакованной директории, чтобы были доступны `spark-config.xml` и скрипты. Основной JAR находится в папке `lib`.
