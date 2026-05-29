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
- Что пришлось исправлять вручную: зависимости go
- Время: ~  20 минут
---
## Задание Повышенной сложности 4: Интеграция Rust-библиотеки для валидации
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Now write a Rust library for data validation. Integrate it into a go-collector with cgo."
**Результат:** rust валидатор, обновленный build.sh и docker файлы. Отображения valid и invalid readings.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: python путь в dockerfile
- Время: ~ 20 минут
---
## Задание Повышенной сложности 5: Развёртывание в Kubernetes с автоскалированием
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "We already packed go-collector into docker image. Now we need to deploy the conveyor into minikube/k3s. Also setup HPA for collector based on queue length or CPU Load."
**Результат:** deploy-k8s.sh скрипт для деплоя, билда img и включения сервера метрик. Все успешно развертывается. Можно просматривать все с помощью logs и get pods/get hpa
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: устанавливал minikube, kubectl. Запуск minikube - minikube start --cpus=4 --memory=4096, выделял больше места.
- Время: ~ 30 минут
---
## Задание Повышенной сложности 6: Сравнение производительности: Go vs Python для сбора
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Now create a python data collector with asyncio, compare its perfomance with Go version of collector under the same load. Results must be a report with graphs."
**Результат:** При запуске bench_compare.py получаем требуемые сравнения и графики.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~ 30 минут
---
## Задание Повышенной сложности 7: Обработка потоковых данных (Kafka)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Replace temp storage in JSON into streaming with kafka. Go collector should write into Topic, python analyzer reads from topic and processes in real time."
**Результат:** Ожидаемый поток данных, го - топик - питон анализатор. Сбор и запуск так же через docker.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~  20 минут
---
## Задание Повышенной сложности 8: Веб-дашборд с обновлением в реальном времени
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** ""
**Результат:** 
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~  минут
---