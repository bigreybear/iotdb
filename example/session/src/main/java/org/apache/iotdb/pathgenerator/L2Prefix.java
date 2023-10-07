package org.apache.iotdb.pathgenerator;

public enum L2Prefix {
    d_,
    TQ,
    C;

    @Override
    public String toString() {
        return name();
    }
}
