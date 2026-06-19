package co.vinod.model;

import javax.management.RuntimeErrorException;

public class Payment {
    private String id;
    private String type;
    private Double amount;
    private String customerId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @Override
    public String toString() {
        return "Payment [id=" + id + ", type=" + type + ", amount=" + amount + ", customerId=" + customerId + "]";
    }

    public static String toCSV(Payment p) {
        return String.format("%s,%s,%s,%s", p.getId(), p.getType(), p.getAmount(), p.getCustomerId());
    }

    public static Payment fromCSV(String csv) {
        try {
            var ar = csv.split(",");
            Payment p = new Payment();
            p.setId(ar[0]);
            p.setType(ar[1]);
            p.setAmount(Double.parseDouble(ar[2]));
            p.setCustomerId(ar[3]);
            return p;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
