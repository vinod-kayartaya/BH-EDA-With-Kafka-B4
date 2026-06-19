package co.vinod.kafka.model;

public class HighValuePayment {

    private String paymentId;
    private String customerId;
    private double amount;
    private String alertType;

    public HighValuePayment() {
    }

    public HighValuePayment(String paymentId,
            String customerId,
            double amount,
            String alertType) {

        this.paymentId = paymentId;
        this.customerId = customerId;
        this.amount = amount;
        this.alertType = alertType;
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

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    @Override
    public String toString() {
        return "HighValuePayment{" +
                "paymentId='" + paymentId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", alertType='" + alertType + '\'' +
                '}';
    }
}