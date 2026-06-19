package co.vinod.loyalty.v1;

import co.vinod.loyalty.avro.v1.CustomerPurchase;
import co.vinod.loyalty.common.util.BannerPrinter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

public class PurchaseReader {

    private static final String INPUT_FILE = "purchase.avro";

    public static void main(String[] args) {

        BannerPrinter.print("AVRO FILE READER");

        try {

            SpecificDatumReader<CustomerPurchase> datumReader = new SpecificDatumReader<>(
                    CustomerPurchase.class);

            DataFileReader<CustomerPurchase> dataFileReader = new DataFileReader<>(
                    new File(INPUT_FILE),
                    datumReader);

            while (dataFileReader.hasNext()) {

                CustomerPurchase purchase = dataFileReader.next();

                System.out.println();
                System.out.println("Purchase Details");
                System.out.println("-------------------------");

                System.out.println(
                        "Customer ID     : "
                                + purchase.getCustomerId());

                System.out.println(
                        "Purchase Amount : "
                                + purchase.getPurchaseAmount());

                System.out.println(
                        "Points Earned   : "
                                + purchase.getPointsEarned());
            }

            dataFileReader.close();

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}