# YDB Copy Table Sample

Пример Java-приложения для экспорта и импорта таблиц YDB с использованием Apache Spark и YDB Spark Connector. Подключается к YDB через Catalog API и использует Parquet в качестве промежуточного формата хранения.

## Принципы работы

Приложение работает в двух режимах:

1. **Export** — читает данные из таблицы YDB через Spark SQL, при необходимости применяя фильтр `WHERE`, и записывает результат в каталог Parquet на локальной файловой системе.
2. **Import** — читает данные из каталога Parquet, при необходимости применяет фильтр `WHERE` и вставляет результат в таблицу YDB.

### Как это устроено

- **Интеграция с каталогом**: YDB представлен как каталог Spark SQL с именем `src`. Конфигурация каталога (URL подключения, аутентификация и т.д.) загружается из XML-файла свойств.
- **Экспорт**: `SELECT * FROM src.<table> [WHERE ...]` → Spark DataFrame → файлы Parquet (режим перезаписи).
- **Импорт**: каталог Parquet → Spark DataFrame → временное представление → `INSERT INTO src.<table> SELECT * FROM parquet_src [WHERE ...]`.
- **Именование таблиц**: пути к таблицам YDB используют точки (например, `database.schema.table`). Слэши в имени таблицы автоматически заменяются на точки.

## Требования

- Java 17
- Maven 3.6+
- Экземпляр YDB (локальный Docker или удалённый)

## Конфигурация

Отредактируйте `scripts/spark-config.xml` (или ваш конфигурационный файл) для задания параметров подключения к YDB:

- **spark.sql.catalog.src** — класс реализации каталога YDB
- **spark.sql.catalog.src.url** — строка подключения к YDB (например, `grpc://localhost:2136/local` для локального Docker)
- **spark.sql.catalog.src.auth.token.file** — путь к файлу токена для облачного/удалённого YDB (раскомментируйте при необходимости)
- **spark.sql.catalog.src.auth.ca.file** — путь к TLS-сертификату для защищённых соединений (раскомментируйте при необходимости)

Подробнее см. [документацию YDB Spark](https://ydb.tech/docs/ru/integrations/ingestion/spark).

## Сборка

```bash
mvn clean package
```

В результате создаётся `target/ydb-copier-1.0-SNAPSHOT.jar` и, при использовании release-профиля, `target/ydb-copier-1.0-SNAPSHOT-bin.zip` с JAR, зависимостями и скриптами.

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
mvn exec:java -Dexec.args="--config spark-config.xml --mode export --table mydb.myschema.mytable --directory /tmp/parquet-out"
```

**Экспорт с фильтром:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode export --table mydb.myschema.mytable --directory /tmp/parquet-out --filter 'id>100 AND status=\"active\"'"
```

**Импорт Parquet в YDB:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode import --table mydb.myschema.mytable --directory /tmp/parquet-in"
```

**Импорт с фильтром:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode import --table mydb.myschema.mytable --directory /tmp/parquet-in --filter 'year=2024'"
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
