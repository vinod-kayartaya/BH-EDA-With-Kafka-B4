package co.vinod.kafka.model;

public class PaymentMethodSummary {

    private String paymentMethod;
    private long count;

    public PaymentMethodSummary() {
    }

    public PaymentMethodSummary(String paymentMethod,
            long count) {

        this.paymentMethod = paymentMethod;
        this.count = count;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PaymentMethodSummary{" +
                "paymentMethod='" + paymentMethod + '\'' +
                ", count=" + count +
                '}';
    }
}