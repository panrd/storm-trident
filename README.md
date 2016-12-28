# Тест Storm Trident топологии
Ситуация:
<br>
На входе есть Kafka-топик `log-topic`, куда складываются некие логи. Формат сообщения в топике JSON-список, в котором >= 0 записей, для каждой из записей ожидаются поля timestamp, host, level и text.

    [
        {
            timestamp:11112222,
            host:"http://host1",
            level:"ERROR",
            text:"Error message"
        },
        {
            timestamp:112123432,
            host:"http://host2",
            level:"DEBUG",
            text:"Debug message"
        },
        .....
        {
            timestamp:1111333,
            host:"http://hostN",
            level:"INFO",
            text:"Info message"
        }
    ]

Топология скользящим окном длительностью 60 секунд высчитает среднюю частоту поступления (количество событий в секунду) и общее количество событий по окну для каждого из уровней:

<ul>
<li>TRACE</li>
<li>DEBUG</li>
<li>INFO</li>
<li>WARN</li>
<li>ERROR</li>
</ul>

Эти величины пишутся по каждому хосту и уровню в HBase.
Если частота поступления по уровню `ERROR` превышает порог 1 событие в секунду,
в Kafka-топик `alert-topic` необходимо записать событие об этом в формате JSON с полями `host` и `error_rate`

#Описание топологии
Топология представляет из себя streaming-pipeline состоящий из стадий:

1. Чтение сообщений из топика `log-topic`
2. Десериализация json в объектную модель `LogMessage`
3. Накапливание сообщений с помощью оконной функции длительностью и шагом 1 минута и сохранение входных кортежей в таблицу HBase `window_tuples_t1`
4. Расчёт частоты сообщений по каждому из уровней и сохранение результата в таблицу HBase `log-history`
5. Фильтрация сообщений по частоте и по уровню
6. Сериализация результата
7. Отправка результата в kafka-топик `alert-topic`

#Что нужно для запуска

Для запуска топологии в **локальном режиме** необходимо 2 docker-контейнера: Kafka и HBase

Образы контейнеров можно скачать себе на локальную машину с docker-демоном с центрального репозитория (по-умолчанию - docker.io) командами:

`docker pull zkhs/kafka:1`

`docker pull zkhs/hbase:1`
 
Оба контейнера содержат Centos 6.8, JDK 1.8 и Zookeeper 3.4.9

Контейнер с Kafka:
<ul>
<li>версия: 2_11.0.10.1.0</li>
<li>zookeeper-порт: 2181</li>
<li>порт брокера: 9092</li>
</ul>

Контейнер HBase:

<ul>
<li>версия: 1.2.4</li>
<li>zookeeper-порт: 2181</li>
<li>Namenode порт (HDFS): 9000</li>
<li>Namenode Web UI порт: 50070</li>
<li>HBase Master порт: 16000</li>
<li>HBase Master Web UI порт: 16010</li>
<li>HBase Regionserver порт: 16020</li>
<li>HBase Regionserver Web UI порт: 16030</li>
</ul>

Если оба контейнера запускаются на одной хост-машине (docker-демоне), то необходимо на той машине, где запускается топология (скорее всего на windows) в hosts файл прописать IP хост-машины и указать имя хоста соответствующее имени хоста контейнеров.

**Пример.**

Запуск контейнеров:

`docker run -p 24:22 -p 2185:2181 -p 9092:9092 --env KAFKA_ADVERTISED_LISTENERS=hbasehost -h hbasehost zkhs/kafka:1 &`

`docker run -d -p 2123:2122 -p 2181:2181 -p 50070:50070 -p 16000:16000 -p 16010:16010 -p 16020:16020 -p 16030:16030 -h hbasehost  zkhs/hbase:1 &`

hosts файл:

`192.168.173.131 hbasehost`

**hbasehost** - hostname запущенных контейнеров

**192.168.173.131** - IP машины, на которой установлен докер демон (docker-engine)

Провериить состояние кластера HBase можно зайдя на:

<a href="http://hbasehost:50070">Namenode - http://hbasehost:50070</a><br>
<a href="http://hbasehost:16010">Master - http://hbasehost:16010</a><br>
<a href="http://hbasehost:16030">Regionserver - http://hbasehost:16030</a>

Перед стартом топологии нужно выполнить maven-задачу: verify - для создания необходимых топиков и таблиц.

**BACKLOG**

Для облегчения процесса отладки, тестирования предполагается автоматическое поднятие докер контейнеров, сконфигурированных в pom-файлах каждого модуля.

Суть задачи в написании docker-maven-plugin'а, который инкапсулирует выкачивание, конфигурирование, запуск и останов контейнеров в соответствии с заданной конфигурацией.
В pom-файле модуля указываются все наобходимые для запуска контейнера параметры: порты, которые требуется пробросить наружу, параметры запуска, hostname контейнера и т.п.

Запуск контейнера должен выполняться на фазе `pre-integration-test`

На фазе `post-integration-test` происходит остановка контейнера.

При конфигурировании хост-машины с докер демоном для подобной задачи необходимо дать возможность связи докер-демона с REST клиентом (docker-maven-plugin'ом).
Для этого, в файле `/lib/systemd/system/docker.service` параметр запуска должен выглядеть примерно так:

`ExecStart=/usr/bin/dockerd -H unix:///var/run/docker.sock -H tcp://[IP машины с докером] -H tcp://127.0.0.1`

**Banchmarks**

Платформа, на которой было проведено тестирование:

HBase: VMWare Player, Centos 6.8, CPU , 1Gb Xmx

Kafka: VMWare Player, Centos 6.8, CPU , 1Gb Xmx

Storm: локальная топология, Xmx4G, CPU Core i5 2.3GHz

Источник сообщений генерирует сообщения, которые пишутся в kafka топик из 5 потоков с интервалом 100мс и в объёме от 0 до 10000 сообщений в каждом из потоков (мат. ожидание = 5000 сообщений/поток)
Trident топология собирает сообщения скользящим окном длительностью 10 секунд, т.о. в среднем одним окном обрабатывается около 500000 сообщений.  

Алгоритм расчёта частоты сообщений реализует линейную сложность.

Сориентироваться по производительности топологии для данной задачи можно по логам:

<pre>
SUCCEED STORE 50 MESSAGES - <i>кол-во сообщений сохранённых в HBase</i>
376 ms - <i>время работы алгоритма</i>
Approx throughput:823969 msg/sec - <i>грубая оценка производительности в 1 сек</i>
Messages proceed number: 310000 - <i>общее кол-во обработанных сообщений из топика одном окном</i>

SUCCEED STORE 50 MESSAGES
114 ms
Approx throughput:1760390 msg/sec
Messages proceed number: 200000

SUCCEED STORE 50 MESSAGES
14 ms
Approx throughput:694429 msg/sec
Messages proceed number: 10000

SUCCEED STORE 50 MESSAGES
50 ms
Approx throughput:2612177 msg/sec
Messages proceed number: 130000

SUCCEED STORE 50 MESSAGES
280 ms
Approx throughput:1463761 msg/sec
Messages proceed number: 410000

SUCCEED STORE 50 MESSAGES
77 ms
Approx throughput:1557991 msg/sec
Messages proceed number: 120000

SUCCEED STORE 50 MESSAGES
220 ms
Approx throughput:1818172 msg/sec
Messages proceed number: 400000

SUCCEED STORE 50 MESSAGES
40 ms
Approx throughput:2241447 msg/sec
Messages proceed number: 90000

SUCCEED STORE 50 MESSAGES
123 ms
Approx throughput:2037102 msg/sec
Messages proceed number: 250000

SUCCEED STORE 50 MESSAGES
141 ms
Approx throughput:1911428 msg/sec
Messages proceed number: 270000

SUCCEED STORE 50 MESSAGES
206 ms
Approx throughput:1310817 msg/sec
Messages proceed number: 270000

SUCCEED STORE 50 MESSAGES
215 ms
Approx throughput:884112 msg/sec
Messages proceed number: 190000
</pre>