# Grafana configuration in this project

Эта директория не содержит исходный код Grafana. Grafana запускается из готового образа `grafana/grafana-oss:11.3.0`.

Мы пишем только конфигурацию и стартовые dashboard'ы.

## `provisioning/datasources/clickhouse.yml`

Provisioning datasource.

Простыми словами: при старте Grafana автоматически создает подключение к ClickHouse. Поэтому пользователю не нужно руками заходить в UI и настраивать host, port, user, password.

Datasource получает uid `clickhouse-analytics`. Этот uid используют dashboard JSON и generated dashboards.

## `provisioning/dashboards/dashboards.yml`

Provisioning dashboard provider.

Он говорит Grafana: "читай dashboard JSON из `/var/lib/grafana/dashboards` и показывай их в папке `Agentic Data Stack`".

В `docker-compose.yml` этот путь смонтирован из локальной директории `./grafana/dashboards`.

## `dashboards/agentic-data-stack-events.json`

Стартовый dashboard в формате JSON.

Dashboard JSON - это сохраненное описание dashboard: панели, SQL-запросы, размеры, цвета, datasource uid.

Grafana UI умеет экспортировать dashboard в такой JSON, а provisioning умеет импортировать его обратно при старте контейнера.

Обычные комментарии в `.json` добавлять нельзя, иначе Grafana не сможет прочитать dashboard. Поэтому пояснение лежит в этом README.

## Мы писали Grafana с нуля?

Нет. Саму Grafana мы не писали.

Мы:

- взяли готовый Docker image Grafana;
- подключили ClickHouse datasource через provisioning;
- добавили стартовый dashboard JSON;
- открыли порт `3001`;
- разрешили модели/MCP создавать новые dashboard'ы через Grafana HTTP API.
