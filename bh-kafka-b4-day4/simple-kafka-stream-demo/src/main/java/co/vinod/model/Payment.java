package co.vinod.model;

public class Payment {

    private long tx_id;
    private String paymentType;
    private double amount;
    private String cust_id;
    private String remarks;

    public Payment() {
    }

    public long getTx_id() {
        return tx_id;
    }

    public void setTx_id(long tx_id) {
        this.tx_id = tx_id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCust_id() {
        return cust_id;
    }

    public void setCust_id(String cust_id) {
        this.cust_id = cust_id;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
}