# Тест Storm Trident топологии
Ситуация:
На входе есть Kafka-топик, куда складываются некие логи. Формат сообщения в топике: JSON-список, в котором >= 0 записей, для каждой из записей ожидаются поля timestamp, host, level и text.
Топология скользящим окном длительностью 60 секунд высчитает среднюю частоту поступления (количество событий в секунду) и общее количество событий по окну для каждого из уровней: TRACE, DEBUG, INFO, WARN, ERROR. Эти величины пишутся по каждому хосту и уровню в HBase.
Если частота поступления по уровню ERROR превышает порог 1 событие в секунду, в Kafka-топик alerts необходимо записать событие об этом в формате JSON с полями host и error_rate.