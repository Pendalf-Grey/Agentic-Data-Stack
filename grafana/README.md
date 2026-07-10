# Grafana configuration in this project

Эта директория не содержит исходный код Grafana. Grafana запускается из готового образа `grafana/grafana-oss:11.3.0`.

Мы пишем только конфигурацию datasource и dashboard-шаблоны для API/MCP.

## `provisioning/datasources/clickhouse.yml`

Provisioning datasource.

Простыми словами: при старте Grafana автоматически создает подключение к ClickHouse. Поэтому пользователю не нужно руками заходить в UI и настраивать host, port, user, password.

Datasource получает uid `clickhouse-analytics`. Этот uid используют dashboard JSON и generated dashboards.

## `provisioning/dashboards/dashboards.yml`

Dashboard provisioning сейчас отключен.

Простыми словами: Grafana не пытается сама импортировать dashboard JSON при старте.
Это важно для demo-ветки, потому что Kimi и Grafana MCP создают/обновляют dashboard'ы
динамически через Grafana HTTP API. Если одновременно включить файловый provisioning и
API-создание с одним и тем же `uid`, Grafana начинает конфликтовать.

Файл `dashboards.yml` поэтому содержит пустой список providers.

## `dashboards/agentic-data-stack-events.json`

Dashboard-шаблон в формате JSON.

Dashboard JSON - это сохраненное описание dashboard: панели, SQL-запросы, размеры, цвета, datasource uid.

Grafana UI умеет экспортировать dashboard в такой JSON. В этой demo-ветке такой JSON
можно использовать как шаблон для API/MCP-создания dashboard.

Обычные комментарии в `.json` добавлять нельзя, иначе Grafana не сможет прочитать dashboard. Поэтому пояснение лежит в этом README.

## Мы писали Grafana с нуля?

Нет. Саму Grafana мы не писали.

Мы:

- взяли готовый Docker image Grafana;
- подключили ClickHouse datasource через provisioning;
- добавили dashboard JSON-шаблон;
- открыли порт `3001`;
- разрешили модели/MCP создавать новые dashboard'ы через Grafana HTTP API.
