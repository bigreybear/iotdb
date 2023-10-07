package org.apache.iotdb.pathgenerator;

import java.util.*;
import java.util.stream.Collectors;

public class PathGenerator {
    public static final Random RANDOM = new Random();
    private static final int MAX_DEVICE_SUFFIX = 99999999;

    private int expectedSize, devSize, sensorSize;
    private String[] paths;

    public PathGenerator() {}

    public PathGenerator(int devSize, int sensorSize) {
        this.devSize = devSize;
        this.sensorSize = sensorSize;
        expectedSize = devSize * sensorSize;
        paths = new String[expectedSize];
    }

    public Iterator<String> getIte() {
        return Arrays.asList(paths).iterator();
    }

    public static String getStringNumber(int width, int value) {
        return String.format("%0" + width + "d", value);
    }

    public static List<String> getRandomSensors(int size) {
        List<String> elems = Arrays.stream(MeasurementName.values()).map(MeasurementName::toString).collect(Collectors.toList());
        Collections.shuffle(elems);
        return elems.subList(0, size);
    }

    public static <T extends Enum<?>> T getRandomEnum (Class<T> enumClass) {
        return enumClass.getEnumConstants()[RANDOM.nextInt(enumClass.getEnumConstants().length)];
    }

    public void generate() {
        long timer = System.currentTimeMillis();
        int cnt = 0;
        String[] pathComp = new String[4];
        pathComp[0] = "root";

        pathComp[1] = getRandomEnum(L1Prefix.class).toString();

        StringBuilder sb = new StringBuilder(getRandomEnum(L2Prefix.class).toString());
        int sbLength = sb.length();
        int devNumber = RANDOM.nextInt(MAX_DEVICE_SUFFIX);

        List<String> sensors = getRandomSensors(sensorSize);


        while (cnt < expectedSize) {
            pathComp[2] = sb.append(getStringNumber(8, devNumber)).toString();
            for (String sensor : sensors) {
                pathComp[3] = sensor;
                paths[cnt] = String.join(".", pathComp);
                cnt++;
            }
            sb.setLength(sbLength);
            devNumber = devNumber == MAX_DEVICE_SUFFIX ? 0 : devNumber + 1;


            if (RANDOM.nextFloat() > 0.95) {
                pathComp[1] = getRandomEnum(L1Prefix.class).toString();
                sb.setLength(0);
                sb.append(getRandomEnum(L2Prefix.class).toString());
                sbLength = sb.length();
            }
        }

        timer = System.currentTimeMillis() - timer;
        System.out.println(String.format("Generating for %d mil-seconds", timer));
    }

    public static void main(String[] args) {
//        Random seed = new Random();
//        int mSize = MeasurementName.values().length;
//        int chosen = seed.nextInt(mSize);
//        System.out.println(MeasurementName.values()[chosen]);
        System.out.println(getRandomEnum(MeasurementName.class));
////        System.out.println("hellp");
//
//        System.out.println(getStringNumber(8, chosen));
//        System.out.println(PathGenerator.getRandomSensors(20));

        PathGenerator pg1 = new PathGenerator(40000, 25);
        pg1.generate();
        Iterator<String> res = pg1.getIte();
        while (res.hasNext()) {
            System.out.println(res.next());
        }
    }
}
