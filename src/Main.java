import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesException;
import java.time.zone.ZoneRulesProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args) {
        var tzdb1 = new TzdbZoneRulesProvider(args[0]);
        System.out.println("ver: " + tzdb1.versionId);
        System.out.println("regionIds: " + tzdb1.regionIds);
        System.out.println("regionToRules: " + tzdb1.regionToRules);
        var tzdb2 = new TzdbZoneRulesProvider(args[1]);
        System.out.println("ver: " + tzdb2.versionId);
        System.out.println("regionIds: " + tzdb2.regionIds);
        System.out.println("regionToRules: " + tzdb2.regionToRules);

        diff(tzdb2.regionIds, tzdb1.regionIds);

        var diffIds = tzdb2.regionIds.stream()
                .filter(id -> !tzdb1.provideRules(id, true).equals(tzdb2.provideRules(id, true)))
                .toList();
        System.out.println("ids whose rules differ: " + diffIds);
        System.out.println("rule1: " + tzdb1.provideRules("Africa/Abidjan", true).getTransitions());
        System.out.println("rule1: " + tzdb2.provideRules("Africa/Abidjan", true).getTransitions());
    }

    private static Set<String> diff(Collection<? extends String> expected, Collection<? extends String> actual) {
        Set<String> s1 = new TreeSet<>(expected);
        s1.removeAll(actual);
        if (!s1.isEmpty()) {
            System.out.println("\tMissing locale(s): " + s1);
        }
        Set<String> s2 = new TreeSet<>(actual);
        s2.removeAll(expected);
        if (!s2.isEmpty()) {
            System.out.println("\tExtra locale(s): " + s2);
        }
        return s2;
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
            throw new ZoneRulesException("Unknown time-zone ID: " + zoneId);
        }
        try {
            if (obj instanceof byte[] bytes) {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                obj = Ser.read(dis);
                regionToRules.put(zoneId, obj);
            }
            return (ZoneRules) obj;
        } catch (Exception ex) {
            throw new ZoneRulesException("Invalid binary time-zone data: TZDB:" + zoneId + ", version: " + versionId, ex);
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
     * Serialization version.
     */
    private static final long serialVersionUID = -8885321777449118786L;

    /** Type for ZoneRules. */
    static final byte ZRULES = 1;
    /** Type for ZoneOffsetTransition. */
    static final byte ZOT = 2;
    /** Type for ZoneOffsetTransitionRule. */
    static final byte ZOTRULE = 3;

    /** The type being serialized. */
    private byte type;
    /** The object being serialized. */
    private Serializable object;

    /**
     * Constructor for deserialization.
     */
    public Ser() {
    }

    static Serializable read(DataInput in) throws IOException, ClassNotFoundException {
        byte type = in.readByte();
        try {
            return readInternal(type, in);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Serializable readInternal(byte type, DataInput in)
            throws IOException, ClassNotFoundException, Throwable {
        MethodHandle zrre = MethodHandles.privateLookupIn(ZoneRules.class, MethodHandles.lookup()).findStatic(ZoneRules.class, "readExternal", MethodType.methodType(ZoneRules.class, DataInput.class));
        MethodHandle zotre = MethodHandles.privateLookupIn(ZoneOffsetTransition.class, MethodHandles.lookup()).findStatic(ZoneOffsetTransition.class, "readExternal", MethodType.methodType(ZoneOffsetTransition.class, DataInput.class));
        MethodHandle zotrre = MethodHandles.privateLookupIn(ZoneOffsetTransitionRule.class, MethodHandles.lookup()).findStatic(ZoneOffsetTransitionRule.class, "readExternal", MethodType.methodType(ZoneOffsetTransitionRule.class, DataInput.class));
        return (Serializable) switch (type) {
            case ZRULES -> zrre.invoke(in);
            case ZOT -> zotre.invoke(in);
            case ZOTRULE -> zotrre.invoke(in);
            default -> throw new StreamCorruptedException("Unknown serialized type");
        };
    }

    /**
     * Returns the object that will replace this one.
     *
     * @return the read object, should never be null
     */
    private Object readResolve() {
        return object;
    }

    //-----------------------------------------------------------------------
    /**
     * Writes the state to the stream.
     *
     * @param offset  the offset, not null
     * @param out  the output stream, not null
     * @throws IOException if an error occurs
     */
    static void writeOffset(ZoneOffset offset, DataOutput out) throws IOException {
        final int offsetSecs = offset.getTotalSeconds();
        int offsetByte = offsetSecs % 900 == 0 ? offsetSecs / 900 : 127;  // compress to -72 to +72
        out.writeByte(offsetByte);
        if (offsetByte == 127) {
            out.writeInt(offsetSecs);
        }
    }

    /**
     * Reads the state from the stream.
     *
     * @param in  the input stream, not null
     * @return the created object, not null
     * @throws IOException if an error occurs
     */
    static ZoneOffset readOffset(DataInput in) throws IOException {
        int offsetByte = in.readByte();
        return (offsetByte == 127 ? ZoneOffset.ofTotalSeconds(in.readInt()) : ZoneOffset.ofTotalSeconds(offsetByte * 900));
    }

    //-----------------------------------------------------------------------
    /**
     * Writes the state to the stream.
     *
     * @param epochSec  the epoch seconds, not null
     * @param out  the output stream, not null
     * @throws IOException if an error occurs
     */
    static void writeEpochSec(long epochSec, DataOutput out) throws IOException {
        if (epochSec >= -4575744000L && epochSec < 10413792000L && epochSec % 900 == 0) {  // quarter hours between 1825 and 2300
            int store = (int) ((epochSec + 4575744000L) / 900);
            out.writeByte((store >>> 16) & 255);
            out.writeByte((store >>> 8) & 255);
            out.writeByte(store & 255);
        } else {
            out.writeByte(255);
            out.writeLong(epochSec);
        }
    }

    /**
     * Reads the state from the stream.
     *
     * @param in  the input stream, not null
     * @return the epoch seconds, not null
     * @throws IOException if an error occurs
     */
    static long readEpochSec(DataInput in) throws IOException {
        int hiByte = in.readByte() & 255;
        if (hiByte == 255) {
            return in.readLong();
        } else {
            int midByte = in.readByte() & 255;
            int loByte = in.readByte() & 255;
            long tot = ((hiByte << 16) + (midByte << 8) + loByte);
            return (tot * 900) - 4575744000L;
        }
    }

}

