package co.vinod.kafka.config;

import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {

    private static final Properties props = new Properties();

    static {
        try (InputStream is = KafkaConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is == null) {
                throw new RuntimeException("Could not find application.properties");
            }
            props.load(is);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}
