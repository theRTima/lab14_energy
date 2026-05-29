## Задание Повышенной сложности 1: Распределённый сборщик на Go (координация через etcd)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Task sphere - energy consumption analyze. Data source - emulated electricity meters. Implement a system where multiple Go collector instances can run in parallel (each collecting its own portion of the data). Use etcd to coordinate and distribute shards/sources."
**Результат:** Проект запускаемый через docker compose. 3 go коллектора с возможностью добавления новых.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: конфликты версий и зависимостей go
- Время: ~ 20 минут
---
## Задание Повышенной сложности 2: Оконная агрегация в Go перед отправкой в Python
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Now add a tumbling window to the collector. For example: every N time interval or every M records (Power Analysis), aggregate the data on the Go side and send aggregated data (sums, averages, etc.) to Python, rather than the original records, to reduce the volume of transferred data."
**Результат:** Обновления docker файла, запуск локального сервера с просмотром статистики по адресу http://localhost:5001/stats .
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: порт сервера
- Время: ~  10 минут
---
## Задание Повышенной сложности 3:Передача данных через Apache Arrow
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Change file transfer from json file to Apache Arrow with FlightRPC. Make a go server which gives data in arrow format and python client which accepts it."
**Результат:** Отправка данных на Fligth сервер с DoPut и доставка данных python клиентм с помощью DoGet. Запуск сервера на порту 8815
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~  минут
---