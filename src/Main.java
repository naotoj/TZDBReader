import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesException;
import java.time.zone.ZoneRulesProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * args[0]: Directory path for the first tzdb.dat file
 * args[1]: Directory path for the second tzdb.dat file
 */
public class Main {
    public static void main(String[] args) {
        TzdbZoneRulesProvider tzdbs[] = new TzdbZoneRulesProvider[2];
        tzdbs[0] = new TzdbZoneRulesProvider(args[0]);
        tzdbs[1] = new TzdbZoneRulesProvider(args[1]);
        Arrays.stream(tzdbs).forEach(tzdb -> {
            System.out.println("ver: " + tzdb.versionId);
//            System.out.println("\tregionIds: " + tzdb.regionIds);
//            System.out.println("\tregionToRules: " + tzdb.regionToRules);
        });

        diff(tzdbs[0].regionIds, tzdbs[1].regionIds);

        var diffIds = tzdbs[1].regionIds.stream()
                .filter(id -> !Objects.equals(tzdbs[0].provideRules(id, true), tzdbs[1].provideRules(id, true)))
                .toList();
        System.out.println("IDs whose rules differ: " + diffIds);
        diffIds.stream().forEach(id -> {
            System.out.println("id: " + id);
            Arrays.stream(tzdbs).forEach(tzdb -> {
                var zr = tzdb.provideRules(id, true);
                System.out.println("\t" + (zr != null ? zr.getTransitions() : null));
            });
        });
    }

    private static void diff(Collection<? extends String> regionIds0, Collection<? extends String> regionIds1) {
        Set<String> s0 = new TreeSet<>(regionIds0);
        s0.removeAll(regionIds1);
        if (!s0.isEmpty()) {
            System.out.println("\tExtra regionId(s) in tzdb0: " + s0);
        }
        Set<String> s1 = new TreeSet<>(regionIds1);
        s1.removeAll(regionIds0);
        if (!s1.isEmpty()) {
            System.out.println("\tExtra regionId(s) in tzdb1: " + s1);
        }
        if (s0.isEmpty() && s1.isEmpty()) {
            System.out.println("\tTwo regionIds are identical");
        }
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
    public TzdbZoneRulesProvider(String libPath) {
        try {
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(
                            new File(libPath, "tzdb.dat"))))) {
                load(dis);
            }
        } catch (Exception ex) {
            throw new ZoneRulesException("Unable to load TZDB time-zone rules", ex);
        }
    }

    @Override
    protected Set<String> provideZoneIds() {
        return new HashSet<>(regionIds);
    }

    @Override
    protected ZoneRules provideRules(String zoneId, boolean forCaching) {
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
        TreeMap<String, ZoneRules> map = new TreeMap<>();
        ZoneRules rules = getRules(zoneId, false);
        if (rules != null) {
            map.put(versionId, rules);
        }
        return map;
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

