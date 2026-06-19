package co.vinod.loyalty.common.util;

import org.apache.avro.specific.SpecificRecord;

public class AvroRecordPrinter {

    private AvroRecordPrinter() {
    }

    public static void print(SpecificRecord record) {

        System.out.println("------------------------------------");
        System.out.println(record);
        System.out.println("------------------------------------");
    }
}