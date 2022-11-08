package com.example.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Component
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @Value("${fileName:output.txt}")
    private String fileName;

    private PrintStream outputStream;

    private StopWatch stopWatch;

    @PostConstruct
    public void initialize() throws Exception {
        LOGGER.info("FileConsumer initialized");
        if (fileName == null) {
            outputStream = System.out;
        } else {
            try {
                outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                        false,
                        StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                throw new Exception("Couldn't find or create file '" + fileName + "' for FileStreamSinkTask", e);
            }
        }

        stopWatch = new StopWatch("file-consumer");
    }

    @KafkaListener(topics = {"${topics:}"}, groupId = "group",
            containerFactory = "logEventKafkaListenerContainerFactory")
    public void consumeLogEvent(List<Object> records) {
        if (CollectionUtils.isEmpty(records)) {
            return;
        }

        stopWatch.start();

        for (Object record : records) {
            outputStream.println(record);
        }
        outputStream.flush();

        stopWatch.stop();

        LOGGER.info("pool size : {}, 소요시간 : {}/ms", records.size(), stopWatch.getTotalTimeMillis());
    }
}
