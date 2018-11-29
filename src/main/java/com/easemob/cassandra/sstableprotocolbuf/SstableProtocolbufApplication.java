package com.easemob.cassandra.sstableprotocolbuf;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SstableProtocolbufApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SstableProtocolbufApplication.class);
        application.setWebApplicationType(WebApplicationType.NONE);

        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}
