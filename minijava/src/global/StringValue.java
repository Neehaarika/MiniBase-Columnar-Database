package global;

/**
 * Created by rubinder on 3/16/18.
 */
public class StringValue extends ValueClass {

    private String value;

    public StringValue(String value) {
        this.value = new String(value);
    }

    public String getValue() {
        return new String(value);
    }

    public void setValue(String value) {
        this.value = new String(value);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringValue that = (StringValue) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
