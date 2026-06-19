package co.vinod.kafka.model;

public class AmountSummary {

    private long windowStart;
    private long windowEnd;
    private double totalAmount;

    public AmountSummary() {
    }

    public AmountSummary(long windowStart,
            long windowEnd,
            double totalAmount) {

        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalAmount = totalAmount;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "AmountSummary{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", totalAmount=" + totalAmount +
                '}';
    }
}   