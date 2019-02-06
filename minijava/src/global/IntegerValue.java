package global;

/**
 * Created by rubinder on 3/16/18.
 */
public class IntegerValue extends ValueClass {

    private Integer value;

    public IntegerValue(Integer value) {
        this.value = new Integer(value.intValue());
    }

    public IntegerValue(int value) {
        this.value = new Integer(value);
    }

    public Integer getValue() {
        return new Integer(value.intValue());
    }

    public void setValue(Integer value) {
        this.value = new Integer(value.intValue());
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntegerValue that = (IntegerValue) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
