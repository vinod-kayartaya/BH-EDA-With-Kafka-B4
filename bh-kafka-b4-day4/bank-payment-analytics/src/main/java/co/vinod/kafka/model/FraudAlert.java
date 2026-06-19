package co.vinod.kafka.model;

public class FraudAlert {

    private String customerId;
    private long paymentCount;
    private String alertType;

    public FraudAlert() {
    }

    public FraudAlert(String customerId,
            long paymentCount,
            String alertType) {

        this.customerId = customerId;
        this.paymentCount = paymentCount;
        this.alertType = alertType;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getPaymentCount() {
        return paymentCount;
    }

    public void setPaymentCount(long paymentCount) {
        this.paymentCount = paymentCount;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    @Override
    public String toString() {
        return "FraudAlert{" +
                "customerId='" + customerId + '\'' +
                ", paymentCount=" + paymentCount +
                ", alertType='" + alertType + '\'' +
                '}';
    }
}