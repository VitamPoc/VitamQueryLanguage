package fr.gouv.vitam.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class UUIDTest {
    private static final char VERSION           = 'd';
    private static int NB = 100000;
    
    @Test
    public void testStructure() {
        UUID id = new UUID();
        String str = id.toHex();

        assertEquals(str.charAt(10), VERSION);
        assertEquals(str.length(), 36);
    }

    @Test
    public void testParsing() {
        for (int i = 0; i < NB; i++) {
            UUID id1 = new UUID();
            UUID id2 = new UUID(id1.toHex());
            assertEquals(id1, id2);
            assertEquals(id1.hashCode(), id2.hashCode());
    
            UUID id3 = new UUID(id1.getBytes());
            assertEquals(id1, id3);
            assertEquals(id1.hashCode(), id3.hashCode());
    
            UUID id4 = new UUID(id1.toBase64());
            assertEquals(id1, id4);
            assertEquals(id1.hashCode(), id4.hashCode());
        }
    }

    @Test
    public void testNonSequentialValue() {
        final int n = NB;
        String[] ids = new String[n];

        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toBase64();
        }
        long stop = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(! ids[i-1].equals(ids[i]));
        }
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toHex();
        }
        long stop2 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(! ids[i-1].equals(ids[i]));
        }
        long start4 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toBase64();
        }
        long stop4 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(! ids[i-1].equals(ids[i]));
        }
        long start5 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            ids[i] = new UUID().toHex();
        }
        long stop5 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            assertTrue(! ids[i-1].equals(ids[i]));
        }
        System.out.println("B64: "+(stop-start)+ " vsHex: "+(stop2-start2)+ 
                " vsB64: "+(stop4-start4)+" vxHex: "+(stop5-start5));
    }

    @Test
    public void testGetBytesImmutability() {
        UUID id = new UUID();
        byte[] bytes = id.getBytes();
        byte[] original = Arrays.copyOf(bytes, bytes.length);
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;

        assertTrue(Arrays.equals(id.getBytes(), original));
    }

    @Test
    public void testConstructorImmutability() {
        UUID id = new UUID();
        byte[] bytes = id.getBytes();
        byte[] original = Arrays.copyOf(bytes, bytes.length);

        UUID id2 = new UUID(bytes);
        bytes[0] = 0;
        bytes[1] = 0;

        assertTrue(Arrays.equals(id2.getBytes(), original));
    }

    @Test
    public void testVersionField() {
        UUID generated = new UUID();
        assertEquals(VERSION, generated.getVersion());

        UUID parsed1 = new UUID("dc9c531160d0def10bcecc00014628614b89");
        assertEquals(VERSION, parsed1.getVersion());
    }

    @Test
    public void testHexBase64() {
        UUID parsed1 = new UUID("dc9c531160d0def10bcecc00014628614b89");
        UUID parsed2 = new UUID("3JxTEWDQ3vELzswAAUYoYUuJ");
        assertTrue(parsed1.equals(parsed2));
        UUID generated = new UUID();
        UUID parsed3 = new UUID(generated.getBytes());
        UUID parsed4 = new UUID(generated.toBase64());
        UUID parsed5 = new UUID(generated.toHex());
        UUID parsed6 = new UUID(generated.toString());
        assertTrue(generated.equals(parsed3));
        assertTrue(generated.equals(parsed4));
        assertTrue(generated.equals(parsed5));
        assertTrue(generated.equals(parsed6));
    }
    
    @Test
    public void testMultipleUuid() {
        UUID id1 = new UUID();
        UUID id2 = new UUID();
        UUID id3 = new UUID();
        String ids = UUID.assembleUuids(id1, id2, id3);
        assertTrue(UUID.isMultipleUUID(ids));
        assertFalse(UUID.isMultipleUUID(id1.toString()));
        assertEquals(id1, UUID.getFirst(ids));
        assertEquals(id3, UUID.getLast(ids));
        assertEquals(id2, UUID.getUuids(ids)[1]);
        assertEquals(3, UUID.getUuidNb(ids));
        assertEquals(id1.toString(), UUID.getFirstAsString(ids));
        assertEquals(id3.toString(), UUID.getLastAsString(ids));
    }
    @Test
    public void testPIDField() throws Exception {
        UUID id = new UUID();

        assertEquals(UUID.jvmProcessId(), id.getProcessId());
    }

    @Test
    public void testDateField() {
        UUID id = new UUID();
        assertTrue(id.getTimestamp() > new Date().getTime() - 100);
        assertTrue(id.getTimestamp() < new Date().getTime() + 100);
    }
    
    @Test
    public void testMultipleUUIDs() {
        int nb = 100000;
        UUID[] uuids = new UUID[nb];
        StringBuilder builder = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();
        for (int i = 0; i < nb; i ++) {
            uuids[i] = new UUID();
            builder.append(uuids[i].toString());
            builder2.append(uuids[i].toString());
            builder2.append(' ');
        }
        String ids = builder.toString();
        String ids2 = builder2.toString();
        assertEquals(24*nb, ids.length());
        long start = System.currentTimeMillis();
        UUID[] uuids2 = UUID.getUuids(ids);
        long stop = System.currentTimeMillis();
        assertEquals(nb, uuids2.length);
        assertEquals(nb, UUID.getUuidNb(ids));
        for (int i = 0; i < nb; i ++) {
            assertTrue(uuids[i].equals(uuids2[i]));
        }
        assertTrue(uuids[0].equals(UUID.getFirst(ids)));
        assertTrue(uuids[nb-1].equals(UUID.getLast(ids)));

        assertEquals(25*nb, ids2.length());
        long start2 = System.currentTimeMillis();
        UUID[] uuids3 = UUID.getUuidsSharp(ids2);
        long stop2 = System.currentTimeMillis();
        assertEquals(nb, uuids2.length);
        for (int i = 0; i < nb; i ++) {
            assertTrue(uuids[i].equals(uuids3[i]));
        }
        assertTrue(uuids[0].equals(UUID.getFirst(ids2)));
        System.out.println("Create "+nb+" UUIDs from 1 String in "+(stop-start)+":"+(stop2-start2));
    }

    @Test
    public void testForDuplicates() {
        int n = NB;
        Set<UUID> uuids = new HashSet<UUID>();
        UUID[] uuidArray = new UUID[n];

        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++)
            uuidArray[i] = new UUID();
        long stop = System.currentTimeMillis();
        System.out.println("TimeSequential = "+(stop-start)+" so "+(n*1000/(stop-start))+" Uuids/s");

        for (int i = 0; i < n; i++)
            uuids.add(uuidArray[i]);

        System.out.println("Create "+n+" and get: "+uuids.size());
        assertEquals(n, uuids.size());
        checkConsecutive(uuidArray);
    }
    private void checkConsecutive(UUID[] uuidArray) {
        int n = uuidArray.length;
        int i = 1;
        int largest = 0;
        for (; i < n ; i++) {
            if (uuidArray[i].getTimestamp() > uuidArray[i-1].getTimestamp()) {
                int j = i+1;
                long time = uuidArray[i].getTimestamp();
                for (; j < n ; j++) {
                    if (uuidArray[j].getTimestamp() > time) {
                        if (largest < j-i) {
                            largest = j-i;
                            i = j;
                            break;
                        }
                    }
                }
            }
        }
        System.out.println(largest+" different consecutive elements");
    }

    private static class Generator extends Thread {
        private UUID[] uuids;
        int base;
        int n;

        public Generator(int n, UUID[] uuids, int base) {
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
        int numThreads = Runtime.getRuntime().availableProcessors()+1;
        Thread[] threads = new Thread[numThreads];
        int n = NB*2;
        int step = n/numThreads;
        UUID[] uuids = new UUID[step*numThreads];

        long start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Generator(step, uuids, i*step);
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++)
            threads[i].join();
        long stop = System.currentTimeMillis();

        Set<UUID> uuidSet = new HashSet<UUID>();

        for (int i = 0; i < uuids.length; i++)
            uuidSet.add(uuids[i]);

        assertEquals(uuids.length, uuidSet.size());
        uuidSet.clear();
        System.out.println("TimeConcurrent = "+(stop-start)+" so "+(uuids.length*1000/(stop-start))+" Uuids/s");
        TreeSet<UUID> set = new TreeSet<>(new Comparator<UUID>() {
            public int compare(UUID o1, UUID o2) {
                long t1 = o1.getTimestamp();
                long t2 = o2.getTimestamp();
                if (t1 < t2) {
                    return -1;
                } else if (t1 > t2) {
                    return 1;
                } else {
                    int c1 = o1.getCounter();
                    int c2 = o2.getCounter();
                    return (c1 < c2 ? -1 : (c1 > c2 ? 1 : 0));
                }
            }
            
        });
        for (int i = 0; i < uuids.length; i++)
            set.add(uuids[i]);
        checkConsecutive(set.toArray(new UUID[0]));
    }
}