package co.vinod.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper mapper =
            new ObjectMapper();

    public static String toJson(Object obj) {

        try {
            return mapper.writeValueAsString(obj);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}