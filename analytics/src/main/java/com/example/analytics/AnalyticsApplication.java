package com.example.analytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class AnalyticsApplication {

    @Component
    public static class PageViewEventSource implements ApplicationRunner {

        private final MessageChannel pageViewsOut;

        private static final Logger logger = LoggerFactory.getLogger(PageViewEventSource.class);

        public PageViewEventSource(AnalyticsBinding binding) {
            this.pageViewsOut = binding.pageViewsOut();
        }

        @Override
        public void run(ApplicationArguments args) throws Exception {
            List<String> names = Arrays.asList("jlong", "pwebb", "schacko", "abilan", "grussel");
            List<String> pages = Arrays.asList("blog", "sitemap", "initializr", "news", "colophon", "about");

            Runnable runnable = () -> {
                String rPage = pages.get(new Random().nextInt(pages.size()));
                String rName = pages.get(new Random().nextInt(names.size()));

                PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);

                Message<PageViewEvent> message = MessageBuilder
                        .withPayload(pageViewEvent)
                        .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
                        .build();

                try {
                    this.pageViewsOut.send(message);
                    logger.info("Sent message: " + message.toString());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                }
            };

            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
        }
    }

    @Component
    public static class PageViewEventSink {

        private static final Logger logger = LoggerFactory.getLogger(PageViewEventSink.class);

        @StreamListener
        public void process (@Input (AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
            KTable<Object, Long> kTable = events
                    .filter((key, value) -> value.getDuration() > 10)
                    .map((KeyValueMapper<String, PageViewEvent, KeyValue<?, ?>>) (key, value) -> new KeyValue<>(value.getPage(), "0"))
                    .groupByKey()
                    .count(Materialized.as(AnalyticsBinding.PAGE_VIEWS_MV));
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }

}

interface AnalyticsBinding {

    String PAGE_VIEWS_OUT = "pvout";
    String PAGE_VIEWS_IN = "pvin";
    String PAGE_VIEWS_MV = "pcmv";

    @Input(PAGE_VIEWS_IN)
    KStream<String, PageViewEvent> pageViewsIn();

    @Output(PAGE_VIEWS_OUT)
    MessageChannel pageViewsOut();
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
    private String userId, page;
    private long duration;
}