package co.vinod.kafka.config;

import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {

    private static final Properties props = new Properties();

    static {

        String profile = System.getProperty("profile", "v1");
        String file = profile + "/application.properties";

        try (InputStream is = KafkaConfig.class
                .getClassLoader()
                .getResourceAsStream(file)) {

            props.load(is);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}