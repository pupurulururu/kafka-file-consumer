package com.example.consumer;

import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.gson.Gson;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Component
public class KafkaJdbcConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJdbcConsumer.class);

    String sql = "insert into default.eventTableTest5(eventtime, sid, pcid, uid, event) format RowBinary";

    Gson gson = new Gson();
    DataSource dataSource;

    @PostConstruct
    public void init() throws SQLException {
        String url = "jdbc:clickhouse://localhost:18123/";
        Properties properties = new Properties();
        properties.setProperty("user", "wisecollector");
        properties.setProperty("password", "wisecollector1234");

        dataSource = new ClickHouseDataSource(url, properties);
    }

    @KafkaListener(topics = {"${topics:}"}, groupId = "group",
            containerFactory = "logEventKafkaListenerContainerFactory")
    public void consumeLogEvent(List<Object> records) {
        if (CollectionUtils.isEmpty(records)) {
            return;
        }

        List<EventTableRecord> list = new ArrayList<>();
        records.forEach(record -> {
            addList(list, (String) record);
        });
        //addList(list, (String) records.get(0));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(sql);) {
            ClickHouseWriter clickHouseWriter = null;

            preparedStatement.setObject(1, (ClickHouseWriter) clickHouseOutputStream -> {
                for (EventTableRecord record : list) {
                    clickHouseOutputStream.writeUnicodeString(String.valueOf(record.getEventtime() / 1000L));
                    clickHouseOutputStream.writeUnicodeString(record.getSid());
                    clickHouseOutputStream.writeUnicodeString(record.getPcid());
                    clickHouseOutputStream.writeUnicodeString(record.getUid());
                    clickHouseOutputStream.writeUnicodeString(record.getEvent());
                }
            });
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    private void addList(List<EventTableRecord> list, String record) {
        Map map = gson.fromJson(record, Map.class);
        String pcid = getStringFrom(map, "cookies.pcid");
        String uid = getStringFrom(map, "cookies.uid");
        long eventTime = getDateTimeMilli(map, "datetime");
        String sid = getStringFrom(map, "sid");
        String event = getStringFrom(map, "cookies.event");
        list.add(EventTableRecord.of(eventTime, sid, pcid, uid, event));
    }

    public static long getDateTimeMilli(Object map, String column) {
        if (MapUtils.isEmpty((Map) map)) {
            LOGGER.debug("{}값이 없어서, default [LocalDateTime.now() long]반환합니다", column);
            return getEpochMilli(LocalDateTime.now());
        }

        String dateTimeStr = getStringFrom(map, column);

        if (StringUtils.isEmpty(dateTimeStr)) {
            return getEpochMilli(LocalDateTime.now());
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");

        return getEpochMilli(LocalDateTime.parse(dateTimeStr, formatter));
    }

    public static String getStringFrom(Object map, String column) {
        if (MapUtils.isEmpty((Map) map)) {
            LOGGER.debug("{}값이 없어서, default string value[\"\"]을 반환합니다", column);
            return StringUtils.EMPTY;
        }

        String value = extractValue((Map) map, column);

        if (StringUtils.isEmpty(value)) {
            LOGGER.debug("{}값이 없어서, default string value[\"\"]을 반환합니다", column);
            return StringUtils.EMPTY;
        }

        return value;
    }

    public static String extractValue(Map map, String columnName) {
        if (StringUtils.isEmpty(columnName)) {
            return StringUtils.EMPTY;
        }

        String[] split = columnName.split("\\.");

        if (split.length > 1) {
            Object o = map;

            for (String innerKey : split) {
                if (ObjectUtils.isEmpty(o)) {
                    return StringUtils.EMPTY;
                }

                o = ((Map<?, ?>) o).get(innerKey);
            }

            return (String) o;
        } else {
            String value = (String) map.get(columnName);

            return value == null ? StringUtils.EMPTY : value;
        }
    }

    private static long getEpochMilli(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
