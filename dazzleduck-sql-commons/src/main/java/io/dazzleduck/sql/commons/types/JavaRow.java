package io.dazzleduck.sql.commons.types;

public record JavaRow(Object[] objects) {
    public Object get(int index) {
        return objects[index];
    }

    public void set(int index, Object object) {
        objects[index] = object;
    }
}
