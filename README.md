# balancer
Балансировщик нагрузки
- producer.py - сервис, который генерирует нагрузку. Спамит сообщения в топик balancer, в качестве payload шлет unixtime c милисекундами.
- balancer.py - сервис слушает сообщения от producer.py (topic balancer) и распределяет нагрузку между worker.py (шлёт сообщение в balancer
/worker/1..N). Также анализирует подключение/отключение worker.py
- worker.py - при старте сообщает о себе balancer.py и начинает слушать свой топик balancer/worker/1..N. Получив сообщение перепосчивает
его consumer.py в канал result/worker/1..N
- consumer.py - слушает сообщения топика result/worker/1..N от всех worker.py и выводит в лог информацию о том, какой worker обработал
сообщение.

Всё общение между сервисами осуществляется через протокол MQTT.
Каждый из файлов запускается в отдельной вкладке терминала, worker.py и producer.py можно запустить любое количество, balancer.py и consumer.py только 1.
Перед запуском файлов необходимо установить значения переменным окружения FLESPI_TOKEN и FLESPI_HOST. Их можно получить зарегистрировавшить на https://flespi.io/. 

## Requirements
- python 3.6+
- https://flespi.com/mqtt-broker
- https://github.com/wialon/gmqtt

## Example

```bash
export FLESPI_HOST=ru-mqtt.flespi.io
export FLESPI_TOKEN=JfMtR9L2fvNAQnb2uQxzQoMWW3F2VvhszSLgPn6ViDsmXkeh0zFENhqzaWrTfxcC
python balancer.py
```
