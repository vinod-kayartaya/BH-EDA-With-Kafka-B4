package co.vinod.kafka.model;

public class CitySummary {

    private String city;
    private long count;

    public CitySummary() {
    }

    public CitySummary(String city, long count) {
        this.city = city;
        this.count = count;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CitySummary{" +
                "city='" + city + '\'' +
                ", count=" + count +
                '}';
    }
}