import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesException;
import java.time.zone.ZoneRulesProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * Tool to read contents in tzdb.dat files and compare.
 *
 * args[0]: tzdb.dat file
 * args[1] (optional): second tzdb.dat file. If there's no second
 * tzdb.dat file provided, the first tzdb.dat is compared against
 * the running JVM's ZoneIds
 */
public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.print("Requires at least one tzdb.dat file. Exiting");
            System.exit(-1);
        }
        ZoneRulesProvider[] zrps = new ZoneRulesProvider[2];
        IntStream.range(0, 2).forEach(i -> {
            zrps[i] = i < args.length ? new TzdbZoneRulesProvider(args[i]) : new CurrentZoneRulesProvider();
            System.out.printf("tzdb[%x] ver: %s\n", i, zrps[i] instanceof TzdbZoneRulesProvider tzdb ? tzdb.versionId : "Running JDK");
        });

        diffMissingExtra(zrps);
        diffRules(zrps);
    }

    private static void diffMissingExtra(ZoneRulesProvider[] zrps) {
        System.out.print("\nMissing/Extra ids comparison: ");

        var regionIds0 = getIds(zrps[0]);
        var regionIds1 = getIds(zrps[1]);

        Set<String> s0 = new TreeSet<>(regionIds0);
        s0.removeAll(regionIds1);
        if (!s0.isEmpty()) {
            System.out.println("Extra regionId(s) in tzdb[0]: " + s0);
        }
        Set<String> s1 = new TreeSet<>(regionIds1);
        s1.removeAll(regionIds0);
        if (!s1.isEmpty()) {
            System.out.println("Extra regionId(s) in tzdb[1]: " + s1);
        }
        if (s0.isEmpty() && s1.isEmpty()) {
            System.out.println("Two regionIds are identical");
        }
        System.out.println();
    }

    private static void diffRules(ZoneRulesProvider[] zrps) {
        var regionIds0 = getIds(zrps[0]);
        var regionIds1 = getIds(zrps[1]);

        var diffIds = regionIds1.stream()
                .filter(id -> regionIds0.contains(id) && regionIds1.contains(id))
                .filter(id -> !Objects.equals(getRules(zrps[0], id), getRules(zrps[1], id)))
                .sorted()
                .toList();
        if (!diffIds.isEmpty()) {
            System.out.println("IDs whose rules differ: " + diffIds);
            diffIds.stream()
                    .sorted()
                    .forEach(id -> {
                        System.out.println("id: " + id);
                        Arrays.stream(zrps).forEach(zrp -> {
                            var zr = getRules(zrp, id);
                            System.out.println("\t" + (zr != null ? zr.getTransitions() : null));
                        });
                    });
        } else {
            System.out.println("IDs exist in both tzdb.dat all share the same rules");
        }
    }

    private static Set<String> getIds(ZoneRulesProvider zrp) {
        return zrp instanceof TzdbZoneRulesProvider tzdbzrp ?
            tzdbzrp.provideZoneIds() :
            ((CurrentZoneRulesProvider)zrp).provideZoneIds();
    }

    private static ZoneRules getRules(ZoneRulesProvider zrp, String id) {
        return zrp instanceof TzdbZoneRulesProvider tzdbzrp ?
            tzdbzrp.provideRules(id, true) :
            ((CurrentZoneRulesProvider)zrp).provideRules(id, true);
    }
}

/**
 * Loads time-zone rules for 'TZDB'.
 *
 * @since 1.8
 */
final class TzdbZoneRulesProvider extends ZoneRulesProvider {

    /**
     * All the regions that are available.
     */
    List<String> regionIds;
    /**
     * Version Id of this tzdb rules
     */
    String versionId;
    /**
     * Region to rules mapping
     */
    final Map<String, Object> regionToRules = new ConcurrentHashMap<>();

    /**
     * Creates an instance.
     * Created by the {@code ServiceLoader}.
     *
     * @throws ZoneRulesException if unable to load
     */
    public TzdbZoneRulesProvider(String tzdbFile) {
        try {
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(
                            tzdbFile)))) {
                load(dis);
            }
        } catch (Exception ex) {
            throw new ZoneRulesException("Unable to load TZDB time-zone rules", ex);
        }
    }

    @Override
    public Set<String> provideZoneIds() {
        return new HashSet<>(regionIds);
    }

    @Override
    public ZoneRules provideRules(String zoneId, boolean forCaching) {
        // forCaching flag is ignored because this is not a dynamic provider
        Object obj = regionToRules.get(zoneId);
        if (obj == null) {
            System.out.println("Unknown time-zone ID: " + zoneId);
            return null;
        }
        try {
            if (obj instanceof byte[] bytes) {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                obj = Ser.read(dis);
                regionToRules.put(zoneId, obj);
            }
            return (ZoneRules) obj;
        } catch (Exception ex) {
            System.out.println("Invalid binary time-zone data: TZDB:" + zoneId + ", version: " + versionId);
            return null;
        }
    }

    @Override
    protected NavigableMap<String, ZoneRules> provideVersions(String zoneId) {
        return null;
    }

    /**
     * Loads the rules from a DateInputStream, often in a jar file.
     *
     * @param dis  the DateInputStream to load, not null
     * @throws Exception if an error occurs
     */
    private void load(DataInputStream dis) throws Exception {
        if (dis.readByte() != 1) {
            throw new StreamCorruptedException("File format not recognised");
        }
        // group
        String groupId = dis.readUTF();
        if ("TZDB".equals(groupId) == false) {
            throw new StreamCorruptedException("File format not recognised");
        }
        // versions
        int versionCount = dis.readShort();
        for (int i = 0; i < versionCount; i++) {
            versionId = dis.readUTF();
        }
        // regions
        int regionCount = dis.readShort();
        String[] regionArray = new String[regionCount];
        for (int i = 0; i < regionCount; i++) {
            regionArray[i] = dis.readUTF();
        }
        regionIds = Arrays.asList(regionArray);
        // rules
        int ruleCount = dis.readShort();
        Object[] ruleArray = new Object[ruleCount];
        for (int i = 0; i < ruleCount; i++) {
            byte[] bytes = new byte[dis.readShort()];
            dis.readFully(bytes);
            ruleArray[i] = bytes;
        }
        // link version-region-rules
        for (int i = 0; i < versionCount; i++) {
            int versionRegionCount = dis.readShort();
            regionToRules.clear();
            for (int j = 0; j < versionRegionCount; j++) {
                String region = regionArray[dis.readShort()];
                Object rule = ruleArray[dis.readShort() & 0xffff];
                regionToRules.put(region, rule);
            }
        }
    }

    @Override
    public String toString() {
        return "TZDB[" + versionId + "]";
    }
}

final class Ser {
    /**
     * Constructor for deserialization.
     */
    public Ser() {
    }

    static Serializable read(DataInput in) throws IOException {
        byte type = in.readByte();
        try {
            return readInternal(type, in);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Serializable readInternal(byte type, DataInput in) throws Throwable {
        return (Serializable) switch (type) {
            case 1 -> MethodHandles.privateLookupIn(ZoneRules.class, MethodHandles.lookup())
                    .findStatic(ZoneRules.class, "readExternal",
                            MethodType.methodType(ZoneRules.class, DataInput.class))
                    .invoke(in);
            case 2 -> MethodHandles.privateLookupIn(ZoneOffsetTransition.class, MethodHandles.lookup())
                    .findStatic(ZoneOffsetTransition.class, "readExternal",
                            MethodType.methodType(ZoneOffsetTransition.class, DataInput.class))
                    .invoke(in);
            case 3 -> MethodHandles.privateLookupIn(ZoneOffsetTransitionRule.class, MethodHandles.lookup())
                    .findStatic(ZoneOffsetTransitionRule.class, "readExternal",
                            MethodType.methodType(ZoneOffsetTransitionRule.class, DataInput.class))
                    .invoke(in);
            default -> throw new StreamCorruptedException("Unknown serialized type");
        };
    }
}

final class CurrentZoneRulesProvider extends ZoneRulesProvider {
    @Override
    public Set<String> provideZoneIds() {
        return new HashSet<>(ZoneId.getAvailableZoneIds());
    }

    @Override
    public ZoneRules provideRules(String zoneId, boolean forCaching) {
        return ZoneId.of(zoneId).getRules();
    }

    @Override
    protected NavigableMap<String, ZoneRules> provideVersions(String zoneId) {
        return null;
    }
}
