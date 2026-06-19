package co.vinod.kafka.model;

public class PaymentSummary {

    private long windowStart;
    private long windowEnd;
    private long totalPayments;

    public PaymentSummary() {
    }

    public PaymentSummary(long windowStart,
            long windowEnd,
            long totalPayments) {

        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalPayments = totalPayments;
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

    public long getTotalPayments() {
        return totalPayments;
    }

    public void setTotalPayments(long totalPayments) {
        this.totalPayments = totalPayments;
    }

    @Override
    public String toString() {
        return "PaymentSummary{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", totalPayments=" + totalPayments +
                '}';
    }
}