package com.sensorsdata.flink.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    public static String path = "/config.properties";
    private static Properties prop = new Properties();


    private ConfigUtils() {
        init(path);
    }

    public static ConfigUtils getInstanceNew() {
        return new ConfigUtils();
    }

    private void init(String fileName) {
        try {
            InputStream in = this.getClass().getResourceAsStream(fileName);
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String get(String key) {
        return prop.getProperty(key).toString().trim();
    }

    public String get(String key, String defaultValue) {
        try {
            return prop.getProperty(key).toString().trim();
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        try {
            return Boolean.parseBoolean(prop.getProperty(key).toString().trim());
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public int getInteger(String key) {
        return Integer.valueOf(prop.getProperty(key).toString().trim());
    }

}
