package fr.gouv.vitam.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import fr.gouv.vitam.query.exception.InvalidUuidOperationException;

@SuppressWarnings("javadoc")
public class UUIDTest {
    private static final char VERSION = 'd';
    private static int NB = 50000;

    @Test
    public void testStructure() {
        final UUID id = new UUID();
        final String str = id.toHex();

        assertEquals(str.charAt(10), VERSION);
        assertEquals(str.length(), 36);
    }

    @Test
    public void testParsing() {
        for (int i = 0; i < NB; i++) {
            final UUID id1 = new UUID();
            UUID id2;
            try {
                id2 = new UUID(id1.toHex());
                assertEquals(id1, id2);
                assertEquals(id1.hashCode(), id2.hashCode());

                final UUID id3 = new UUID(id1.getBytes());
                assertEquals(id1, id3);
                assertEquals(id1.hashCode(), id3.hashCode());

                final UUID id4 = new UUID(id1.toBase64());
                assertEquals(id1, id4);
                assertEquals(id1.hashCode(), id4.hashCode());
            } catch (final InvalidUuidOperationException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testNonSequentialValue() {
        final int n = NB / 2;
        final String[] ids = new String[n];

        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toBase64();
        }
        final long stop = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(!ids[i - 1].equals(ids[i]));
        }
        final long start2 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toHex();
        }
        final long stop2 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(!ids[i - 1].equals(ids[i]));
        }
        final long start4 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toBase64();
        }
        final long stop4 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(!ids[i - 1].equals(ids[i]));
        }
        final long start5 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toHex();
        }
        final long stop5 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(!ids[i - 1].equals(ids[i]));
        }
        System.out.println("B64: " + (stop - start) + " vsHex: " + (stop2 - start2) + " vsB64: " + (stop4 - start4) + " vxHex: "
                + (stop5 - start5));
    }

    @Test
    public void testGetBytesImmutability() {
        final UUID id = new UUID();
        final byte[] bytes = id.getBytes();
        final byte[] original = Arrays.copyOf(bytes, bytes.length);
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;

        assertTrue(Arrays.equals(id.getBytes(), original));
    }

    @Test
    public void testConstructorImmutability() {
        final UUID id = new UUID();
        final byte[] bytes = id.getBytes();
        final byte[] original = Arrays.copyOf(bytes, bytes.length);

        try {
            final UUID id2 = new UUID(bytes);
            bytes[0] = 0;
            bytes[1] = 0;

            assertTrue(Arrays.equals(id2.getBytes(), original));
        } catch (final InvalidUuidOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testVersionField() {
        final UUID generated = new UUID();
        assertEquals(VERSION, generated.getVersion());

        try {
            final UUID parsed1 = new UUID("dc9c531160d0def10bcecc00014628614b89");
            assertEquals(VERSION, parsed1.getVersion());
        } catch (final InvalidUuidOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testHexBase64() {
        try {
            final UUID parsed1 = new UUID("dc9c531160d0def10bcecc00014628614b89");
            final UUID parsed2 = new UUID("3JxTEWDQ3vELzswAAUYoYUuJ");
            assertTrue(parsed1.equals(parsed2));
            final UUID generated = new UUID();
            final UUID parsed3 = new UUID(generated.getBytes());
            final UUID parsed4 = new UUID(generated.toBase64());
            final UUID parsed5 = new UUID(generated.toHex());
            final UUID parsed6 = new UUID(generated.toString());
            assertTrue(generated.equals(parsed3));
            assertTrue(generated.equals(parsed4));
            assertTrue(generated.equals(parsed5));
            assertTrue(generated.equals(parsed6));
        } catch (final InvalidUuidOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleUuid() {
        try {
            final UUID id1 = new UUID();
            final UUID id2 = new UUID();
            final UUID id3 = new UUID();
            final String ids = UUID.assembleUuids(id1, id2, id3);
            assertTrue(UUID.isMultipleUUID(ids));
            assertFalse(UUID.isMultipleUUID(id1.toString()));
            assertEquals(id1, UUID.getFirst(ids));
            assertEquals(id3, UUID.getLast(ids));
            assertEquals(id2, UUID.getUuids(ids)[1]);
            assertEquals(3, UUID.getUuidNb(ids));
            assertEquals(id1.toString(), UUID.getFirstAsString(ids));
            assertEquals(id3.toString(), UUID.getLastAsString(ids));
        } catch (final InvalidUuidOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJavaUtilUuid() {
        final UUID id1 = new UUID(true);
        final UUID id2 = new UUID(id1.getMostSignificantBits(), id1.getLeastSignificantBits());
        assertEquals(id1, id2);
        final java.util.UUID javaUuid0 = id1.getJavaUuid();
        assertEquals(javaUuid0.getMostSignificantBits(), id1.getMostSignificantBits());
        assertEquals(javaUuid0.getLeastSignificantBits(), id1.getLeastSignificantBits());
        final java.util.UUID javaUuid = java.util.UUID.randomUUID();
        final UUID id3 = new UUID(javaUuid);
        assertEquals(javaUuid.getMostSignificantBits(), id3.getMostSignificantBits());
        assertEquals(javaUuid.getLeastSignificantBits(), id3.getLeastSignificantBits());
        assertEquals(javaUuid, id3.getJavaUuid());
    }
    
    @Test
    public void testPIDField() throws Exception {
        final UUID id = new UUID();

        assertEquals(UUID.jvmProcessId(), id.getProcessId());
    }

    @Test
    public void testDateField() {
        final UUID id = new UUID();
        assertTrue(id.getTimestamp() > new Date().getTime() - 100);
        assertTrue(id.getTimestamp() < new Date().getTime() + 100);
    }

    @Test
    public void testMultipleUUIDs() {
        try {
            final int nb = 50000;
            final UUID[] uuids = new UUID[nb];
            final StringBuilder builder = new StringBuilder();
            final StringBuilder builder2 = new StringBuilder();
            for (int i = 0; i < nb; i++) {
                uuids[i] = new UUID();
                builder.append(uuids[i].toString());
                builder2.append(uuids[i].toString());
                builder2.append(' ');
            }
            final String ids = builder.toString();
            final String ids2 = builder2.toString();
            assertEquals(24 * nb, ids.length());
            final long start = System.currentTimeMillis();
            final UUID[] uuids2 = UUID.getUuids(ids);
            final long stop = System.currentTimeMillis();
            assertEquals(nb, uuids2.length);
            assertEquals(nb, UUID.getUuidNb(ids));
            for (int i = 0; i < nb; i++) {
                assertTrue(uuids[i].equals(uuids2[i]));
            }
            assertTrue(uuids[0].equals(UUID.getFirst(ids)));
            assertTrue(uuids[nb - 1].equals(UUID.getLast(ids)));

            assertEquals(25 * nb, ids2.length());
            final long start2 = System.currentTimeMillis();
            final UUID[] uuids3 = UUID.getUuidsSharp(ids2);
            final long stop2 = System.currentTimeMillis();
            assertEquals(nb, uuids2.length);
            for (int i = 0; i < nb; i++) {
                assertTrue(uuids[i].equals(uuids3[i]));
            }
            assertTrue(uuids[0].equals(UUID.getFirst(ids2)));
            System.out.println("Create " + nb + " UUIDs from 1 String in " + (stop - start) + ":" + (stop2 - start2));
        } catch (final InvalidUuidOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testForDuplicates() {
        final int n = NB;
        final Set<UUID> uuids = new HashSet<UUID>();
        final UUID[] uuidArray = new UUID[n];

        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            uuidArray[i] = new UUID();
        }
        final long stop = System.currentTimeMillis();
        System.out.println("TimeSequential = " + (stop - start) + " so " + (n * 1000 / (stop - start)) + " Uuids/s");

        for (int i = 0; i < n; i++) {
            uuids.add(uuidArray[i]);
        }

        System.out.println("Create " + n + " and get: " + uuids.size());
        assertEquals(n, uuids.size());
        checkConsecutive(uuidArray);
    }

    private void checkConsecutive(final UUID[] uuidArray) {
        final int n = uuidArray.length;
        int i = 1;
        int largest = 0;
        for (; i < n; i++) {
            if (uuidArray[i].getTimestamp() > uuidArray[i - 1].getTimestamp()) {
                int j = i + 1;
                final long time = uuidArray[i].getTimestamp();
                for (; j < n; j++) {
                    if (uuidArray[j].getTimestamp() > time) {
                        if (largest < j - i) {
                            largest = j - i;
                            i = j;
                            break;
                        }
                    }
                }
            }
        }
        System.out.println(largest + " different consecutive elements");
    }

    private static class Generator extends Thread {
        private final UUID[] uuids;
        int base;
        int n;

        public Generator(final int n, final UUID[] uuids, final int base) {
            this.n = n;
            this.uuids = uuids;
            this.base = base;
        }

        @Override
        public void run() {
            for (int i = 0; i < n; i++) {
                uuids[base + i] = new UUID();
            }
        }
    }

    @Test
    public void concurrentGeneration() throws Exception {
        final int numThreads = Runtime.getRuntime().availableProcessors() + 1;
        final Thread[] threads = new Thread[numThreads];
        final int n = NB * 2;
        final int step = n / numThreads;
        final UUID[] uuids = new UUID[step * numThreads];

        final long start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Generator(step, uuids, i * step);
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        final long stop = System.currentTimeMillis();

        final Set<UUID> uuidSet = new HashSet<UUID>();

        for (int i = 0; i < uuids.length; i++) {
            uuidSet.add(uuids[i]);
        }

        assertEquals(uuids.length, uuidSet.size());
        uuidSet.clear();
        System.out.println("TimeConcurrent = " + (stop - start) + " so " + (uuids.length * 1000 / (stop - start)) + " Uuids/s");
        final TreeSet<UUID> set = new TreeSet<>(new Comparator<UUID>() {
            @Override
            public int compare(final UUID o1, final UUID o2) {
                final long t1 = o1.getTimestamp();
                final long t2 = o2.getTimestamp();
                if (t1 < t2) {
                    return -1;
                } else if (t1 > t2) {
                    return 1;
                } else {
                    final int c1 = o1.getCounter();
                    final int c2 = o2.getCounter();
                    return (c1 < c2 ? -1 : (c1 > c2 ? 1 : 0));
                }
            }

        });
        for (int i = 0; i < uuids.length; i++) {
            set.add(uuids[i]);
        }
        checkConsecutive(set.toArray(new UUID[0]));
    }
}