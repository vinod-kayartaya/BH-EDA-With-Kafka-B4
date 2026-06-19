package co.vinod.loyalty.common.config;

import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {

    private PropertyLoader() {
    }

    public static Properties load(String propertyFile) {

        try {

            Properties props = new Properties();

            InputStream is = PropertyLoader.class
                    .getClassLoader()
                    .getResourceAsStream(propertyFile);

            if (is == null) {
                throw new RuntimeException(
                        "Property file not found: " + propertyFile);
            }

            props.load(is);

            return props;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}