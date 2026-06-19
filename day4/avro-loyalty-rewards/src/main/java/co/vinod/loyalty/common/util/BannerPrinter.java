package co.vinod.loyalty.common.util;

public class BannerPrinter {

    private BannerPrinter() {
    }

    public static void print(String title) {

        System.out.println();
        System.out.println("====================================");
        System.out.println(title);
        System.out.println("====================================");
    }
}