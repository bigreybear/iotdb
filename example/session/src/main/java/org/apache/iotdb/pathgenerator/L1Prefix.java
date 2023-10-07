package org.apache.iotdb.pathgenerator;

public enum L1Prefix {
    track,
    packet,
    DTXY,
    XDB;

    @Override
    public String toString() {
        return name();
    }
}
