package co.vinod.kafka.model;

public class Payment {

    private String paymentId;
    private String customerId;
    private String merchantId;

    private String city;
    private String paymentMethod;

    private double amount;

    private long timestamp;

    public Payment() {
    }

    public Payment(String paymentId,
            String customerId,
            String merchantId,
            String city,
            String paymentMethod,
            double amount,
            long timestamp) {

        this.paymentId = paymentId;
        this.customerId = customerId;
        this.merchantId = merchantId;
        this.city = city;
        this.paymentMethod = paymentMethod;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Payment{" +
                "paymentId='" + paymentId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", city='" + city + '\'' +
                ", paymentMethod='" + paymentMethod + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}