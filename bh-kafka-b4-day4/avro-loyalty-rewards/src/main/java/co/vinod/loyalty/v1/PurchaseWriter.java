package co.vinod.loyalty.v1;

import co.vinod.loyalty.avro.v1.CustomerPurchase;
import co.vinod.loyalty.common.util.BannerPrinter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class PurchaseWriter {

    private static final String OUTPUT_FILE = "purchase.avro";

    public static void main(String[] args) {

        BannerPrinter.print("AVRO FILE WRITER");

        CustomerPurchase purchase = CustomerPurchase.newBuilder()
                .setCustomerId("C1001")
                .setPurchaseAmount(8272.00)
                .setPointsEarned(282)
                .build();

        System.out.println("Record Created");
        System.out.println(purchase);

        try {

            SpecificDatumWriter<CustomerPurchase> datumWriter = new SpecificDatumWriter<>(CustomerPurchase.class);

            DataFileWriter<CustomerPurchase> dataFileWriter = new DataFileWriter<>(datumWriter);

            dataFileWriter.create(
                    purchase.getSchema(),
                    new File(OUTPUT_FILE));

            dataFileWriter.append(purchase);

            dataFileWriter.close();

            System.out.println();
            System.out.println(
                    "Record written successfully to "
                            + OUTPUT_FILE);

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}