/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.utils;


import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import net.iharder.Base64;


/**
 * UUID Generator (also Global UUID Generator) <br>
 * <br>
 * Inspired from com.groupon locality-uuid which used combination of internal counter value - process id - 
 * fragment of MAC address and Timestamp. see https://github.com/groupon/locality-uuid.java <br>
 * <br>
 * But force sequence and take care of errors and improves some performance issues
 *  
 * @author "Frederic Bregier"
 *
 */
public final class UUID {
    
    private static final int KEYSIZE            = 18;
    private static final int KEYB64SIZE            = 24;
    private static final int KEYB16SIZE            = KEYSIZE*2;
    /**
     * Random Generator 
     */
    private static final ThreadLocalRandom RANDOM            = ThreadLocalRandom.current();
    /**
     * So MAX value on 2 bytes
     */
    private static final int MAX_PID            = 65536;
    /**
     * Version to store (to check correctness if future algorithm)
     */
    private static final char VERSION            = 'd';
    /**
     * HEX_CHARS
     */
    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
        'a', 'b', 'c', 'd', 'e', 'f', };
    /**
     * VERSION_DEC
     */
    private static final int VERSION_DEC = asByte(VERSION, '0');

    private static final Pattern MACHINE_ID_PATTERN = Pattern.compile("^(?:[0-9a-fA-F][:-]?){6,8}$");
    private static final int MACHINE_ID_LEN = 6;

    /**
     * 2 bytes value maximum
     */
    private static final int JVMPID = jvmProcessId();
    /**
     * Try to get Mac Address but could be also changed dynamically
     */
    private static final byte[] MAC = macAddress();
    /**
     * Counter part
     */
    private static final AtomicInteger COUNTER = new AtomicInteger(RANDOM.nextInt());
    
    /**
     * real UUID
     */
    private final byte[] uuid;
    
    /**
     * Constructor that generates a new UUID using the current process id, MAC address, and timestamp
     */
    public UUID() {
        final long time = System.currentTimeMillis();
        uuid = new byte[KEYSIZE];

        // atomically
        final int count = COUNTER.incrementAndGet();

        // switch the order of the count in 3 bit segments and place into uuid
        uuid[0] = (byte) (((count & 0x0F) << 4) | ((count & 0xF0) >> 4));
        uuid[1] = (byte) (((count & 0xF00) >> 4) | ((count & 0xF000) >> 12));
        uuid[2] = (byte) (((count & 0xF0000) >> 12) | ((count & 0xF00000) >> 20));
        //uuid[3] = (byte) (((count & 0xF000000) >> 20) | ((count & 0xF0000000) >> 28));

        // copy pid to uuid
        uuid[3]  = (byte) (JVMPID >> 8);
        uuid[4]  = (byte) (JVMPID);


        // place UUID version (hex 'c') in first four bits and piece of MAC in
        // the second four bits
        uuid[5]  = (byte) (VERSION_DEC | (0x0F & MAC[0]));
        // copy rest of mac address into uuid
        uuid[6]  = MAC[1];
        uuid[7]  = MAC[2];
        uuid[8]  = MAC[3];
        uuid[9]  = MAC[4];
        uuid[10]  = MAC[5];

        // copy timestamp into uuid (up to 48 bits so up to 2 200 000 years after Time 0)
        uuid[11] = (byte) (time >> 48);
        uuid[12] = (byte) (time >> 40);
        uuid[13] = (byte) (time >> 32);
        uuid[14] = (byte) (time >> 24);
        uuid[15] = (byte) (time >> 16);
        uuid[16] = (byte) (time >> 8);
        uuid[17] = (byte) (time);
    }

    /**
     * Constructor that takes a byte array as this UUID's content
     * @param bytes UUID content
     */
    public UUID(final byte[] bytes) {
        if (bytes.length != KEYSIZE)
            throw new RuntimeException("Attempted to parse malformed UUID: " + Arrays.toString(bytes));

        uuid = Arrays.copyOf(bytes, KEYSIZE);
    }
    /**
     * Build from String key
     * @param idsource
     */
    public UUID(final String idsource) {
        final String id = idsource.trim();

        int len = id.length();
        if (len == KEYB16SIZE) {
            // HEXA
            uuid = new byte[KEYSIZE];
            final char[] chars = id.toCharArray();
            for (int i = 0, j = 0; i < KEYSIZE; ) {
                uuid[i ++]  = asByte(chars[j ++],  chars[j ++]);
            }
        } else if (len == KEYB64SIZE || len == KEYB64SIZE+1) {
            // BASE64
            try {
                uuid = Base64.decode(id, Base64.URL_SAFE|Base64.DONT_GUNZIP);
            } catch (IOException e) {
                throw new RuntimeException("Attempted to parse malformed UUID: " + id, e);
            }
           } else {
               throw new RuntimeException("Attempted to parse malformed UUID: ("+len+") " + id);
           }
    }
    /**
     * 
     * @param uuids
     * @return the assembly UUID of all given UUIDs
     */
    public static String assembleUuids(UUID ...uuids) {
        StringBuilder builder = new StringBuilder();
        for (UUID uuid : uuids) {
            builder.append(uuid.toString());
        }
        return builder.toString();
    }
    /**
     * 
     * @param idsource
     * @return the array of UUID according to the source (concatenation of UUIDs)
     */
    public static UUID[] getUuids(String idsource) {
        final String id = idsource.trim();
        int nb = id.length()/KEYB64SIZE;
        UUID []uuids = new UUID[nb];
        int beginIndex = 0;
        int endIndex = KEYB64SIZE;
        for (int i = 0; i < nb; i++) {
            uuids[i] = new UUID(id.substring(beginIndex, endIndex));
            beginIndex = endIndex;
            endIndex += KEYB64SIZE;
        }
        return uuids;
    }
    /**
     * 
     * @param idsource
     * @return the number of UUID in this idsource
     */
    public static int getUuidNb(String idsource) {
        return idsource.trim().length()/KEYB64SIZE;
    }
    /**
     * 
     * @param idsource
     * @return true if this idsource represents more than one UUID (path of UUIDs)
     */
    public static boolean isMultipleUUID(String idsource) {
        return idsource.trim().length() > KEYB64SIZE;
    }
    /**
     * 
     * @param idsource
     * @return the last UUID from this idsource
     */
    public static UUID getLast(String idsource) {
        final String id = idsource.trim();
        int nb = id.length()/KEYB64SIZE - 1;
        int pos = KEYB64SIZE*nb;
        return new UUID(id.substring(pos, pos+KEYB64SIZE));
    }
    /**
     * 
     * @param idsource
     * @return the first UUID from this idsource
     */
    public static UUID getFirst(String idsource) {
        final String id = idsource.trim().substring(0, KEYB64SIZE);
        return new UUID(id);
    }
    /**
     * 
     * @param idsource
     * @return the last UUID from this idsource
     */
    public static String getLastAsString(String idsource) {
        final String id = idsource.trim();
        int nb = id.length()/KEYB64SIZE - 1;
        int pos = KEYB64SIZE*nb;
        return id.substring(pos, pos+KEYB64SIZE);
    }
    /**
     * 
     * @param idsource
     * @return the first UUID from this idsource
     */
    public static String getFirstAsString(String idsource) {
        return idsource.trim().substring(0, KEYB64SIZE);
    }
    /**
     * 
     * @param idsource
     * @return the array of UUID according to the source (concatenation of UUIDs separated by '#')
     */
    public static UUID[] getUuidsSharp(String idsource) {
        final String id = idsource.trim();
        int nb = id.length()/(KEYB64SIZE+1)+1;
        UUID []uuids = new UUID[nb];
        int beginIndex = 0;
        int endIndex = KEYB64SIZE;
        for (int i = 0; i < nb; i++) {
            uuids[i] = new UUID(id.substring(beginIndex, endIndex));
            beginIndex = endIndex+1;
            endIndex += KEYB64SIZE+1;
        }
        return uuids;
    }
    private static final byte asByte(final char a, final char b) {
        char a2 = a;
        if (a >= HEX_CHARS[10]) {
            a2 -= HEX_CHARS[10] - 10;
        } else {
            a2 -= HEX_CHARS[0];
        }
        char b2 = b;
        if (b >= HEX_CHARS[10]) {
            b2 -= HEX_CHARS[10] - 10;
        } else {
            b2 -= HEX_CHARS[0];
        }
        return (byte) ((a2 << 4) + b2);
    }

    public final String toBase64() {
        try {
            return Base64.encodeBytes(uuid, Base64.URL_SAFE);
        } catch (IOException e) {
            return Base64.encodeBytes(uuid);
        }
    }

    public final String toHex() {
        final char[] id = new char[KEYB16SIZE];

        // split each byte into 4 bit numbers and map to hex characters
        for (int i = 0, j = 0; i < KEYSIZE; i++) {
            id[j ++]  = HEX_CHARS[(uuid[i]  & 0xF0) >> 4];
            id[j ++]  = HEX_CHARS[(uuid[i]  & 0x0F)];
        }
        return new String(id);
    }
    @Override
    public String toString() {
        return toBase64();
    }

    /**
     * copy the uuid of this UUID, so that it can't be changed, and return it
     * @return raw byte array of UUID
     */
    public byte[] getBytes() {
        return Arrays.copyOf(uuid, KEYSIZE);
    }

    /**
     * extract version field as a hex char from raw UUID bytes
     * @return version char
     */
    public char getVersion() {
        return HEX_CHARS[(uuid[5] & 0xF0) >> 4];
    }

    /**
     * extract process id from raw UUID bytes and return as int
     * @return id of process that generated the UUID, or -1 for unrecognized format
     */
    public int getProcessId() {
        if (getVersion() != VERSION)
            return -1;

        return ((uuid[3] & 0xFF) << 8) | (uuid[4] & 0xFF);
    }

    public int getCounter() {
        int count = uuid[2] & 0xF0 >> 4 << 16;
        count |= uuid[2] & 0x0F << 4 << 16;
        count |= uuid[1] & 0xF0 >> 4 << 8;
        count |= uuid[1] & 0x0F << 4 << 8;
        count |= uuid[0] & 0xF0 >> 4;
        count |= uuid[0] & 0x0F << 4;
        return count;
    }
    /**
     * extract timestamp from raw UUID bytes and return as int
     * @return millisecond UTC timestamp from generation of the UUID, or -1 for unrecognized format
     */
    public long getTimestamp() {
        if (getVersion() != VERSION)
            return -1;

        long time;
        time  = ((long)uuid[11] & 0xFF) << 48;
        time |= ((long)uuid[12] & 0xFF) << 40;
        time |= ((long)uuid[13] & 0xFF) << 32;
        time |= ((long)uuid[14] & 0xFF) << 24;
        time |= ((long)uuid[15] & 0xFF) << 16;
        time |= ((long)uuid[16] & 0xFF) << 8;
        time |= ((long)uuid[17] & 0xFF);
        return time;
    }

    /**
     * extract MAC address fragment from raw UUID bytes, setting missing values to 0,
     * thus the first half byte will be 0, followed by 7 and half bytes
     * of the active MAC address when the UUID was generated
     * @return byte array of UUID fragment, or null for unrecognized format
     */
    public byte[] getMacFragment() {
        if (getVersion() != VERSION)
            return null;

        final byte[] x = new byte[6];

        x[0] = (byte) (uuid[5] & 0x0F);
        x[1] = uuid[6];
        x[2] = uuid[7];
        x[3] = uuid[8];
        x[4] = uuid[9];
        x[5] = uuid[10];

        return x;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof UUID)) return false;
        return (this == o) || Arrays.equals(this.uuid, ((UUID) o).uuid);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(uuid);
    }

    /**
     * 
     * @param length
     * @return a byte array with random values
     */
    public static final byte[] getRandom(final int length) {
        final byte[] result = new byte[length];
        RANDOM.nextBytes(result);
        return result;
    }
    
    /**
     * 
     * @return the mac address if possible, else random values
     */
    public static final byte[] macAddress() {
        try {
            byte[] machineId = null;
            String customMachineId = SystemPropertyUtil.get("fr.gouv.vitam.machineId");
            if (customMachineId != null) {
                if (MACHINE_ID_PATTERN.matcher(customMachineId).matches()) {
                    machineId = parseMachineId(customMachineId);
                }
            }

            if (machineId == null) {
                machineId = defaultMachineId();
            }
            return machineId;
            /*
            byte[] mac = null;
            Enumeration<NetworkInterface> enumset = NetworkInterface.getNetworkInterfaces();
            while (enumset.hasMoreElements()) {
                mac = enumset.nextElement().getHardwareAddress();
                if (mac != null && mac.length >= MACHINE_ID_LEN) {
                    break;
                } else {
                    mac = null;
                }
            }
            // if the machine is not connected to a network it has no active MAC address
            if (mac == null || mac.length < MACHINE_ID_LEN) {
                //System.err.println("No MAC Address found");
                mac = getRandom(6);
            }
            return mac;
            */
        } catch (Exception e) {
            //System.err.println("Could not get MAC address");
            //e.printStackTrace();
            return getRandom(MACHINE_ID_LEN);
        }
    }

    private static final byte[] parseMachineId(final String valueSource) {
        // Strip separators.
        String value = valueSource.replaceAll("[:-]", "");

        byte[] machineId = new byte[MACHINE_ID_LEN];
        for (int i = 0; i < value.length() && i < MACHINE_ID_LEN; i += 2) {
            machineId[i] = (byte) Integer.parseInt(value.substring(i, i + 2), 16);
        }

        return machineId;
    }

    private static final byte[] defaultMachineId() {
        // Find the best MAC address available.
        final byte[] NOT_FOUND = { -1 };
        byte[] bestMacAddr = NOT_FOUND;
        InetAddress bestInetAddr = null;
        try {
            bestInetAddr = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        } catch (UnknownHostException e) {
            // Never happens.
            throw new IllegalArgumentException(e);
        }

        // Retrieve the list of available network interfaces.
        Map<NetworkInterface, InetAddress> ifaces = new LinkedHashMap<NetworkInterface, InetAddress>();
        try {
            for (Enumeration<NetworkInterface> i = NetworkInterface.getNetworkInterfaces(); i.hasMoreElements();) {
                NetworkInterface iface = i.nextElement();
                // Use the interface with proper INET addresses only.
                Enumeration<InetAddress> addrs = iface.getInetAddresses();
                if (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (!a.isLoopbackAddress()) {
                        ifaces.put(iface, a);
                    }
                }
            }
        } catch (SocketException e) {
        }

        for (Entry<NetworkInterface, InetAddress> entry: ifaces.entrySet()) {
            NetworkInterface iface = entry.getKey();
            InetAddress inetAddr = entry.getValue();
            if (iface.isVirtual()) {
                continue;
            }

            byte[] macAddr;
            try {
                macAddr = iface.getHardwareAddress();
            } catch (SocketException e) {
                continue;
            }

            boolean replace = false;
            int res = compareAddresses(bestMacAddr, macAddr);
            if (res < 0) {
                // Found a better MAC address.
                replace = true;
            } else if (res == 0) {
                // Two MAC addresses are of pretty much same quality.
                res = compareAddresses(bestInetAddr, inetAddr);
                if (res < 0) {
                    // Found a MAC address with better INET address.
                    replace = true;
                } else if (res == 0) {
                    // Cannot tell the difference.  Choose the longer one.
                    if (bestMacAddr.length < macAddr.length) {
                        replace = true;
                    }
                }
            }

            if (replace) {
                bestMacAddr = macAddr;
                bestInetAddr = inetAddr;
            }
        }

        if (bestMacAddr == NOT_FOUND) {
            bestMacAddr = getRandom(MACHINE_ID_LEN);
        }
        return bestMacAddr;
    }
    
    /**
     * @return positive - current is better, 0 - cannot tell from MAC addr, negative - candidate is better.
     */
    private static final int compareAddresses(byte[] current, byte[] candidate) {
        if (candidate == null) {
            return 1;
        }
        // Must be EUI-48 or longer.
        if (candidate.length < 6) {
            return 1;
        }
        // Must not be filled with only 0 and 1.
        boolean onlyZeroAndOne = true;
        for (byte b: candidate) {
            if (b != 0 && b != 1) {
                onlyZeroAndOne = false;
                break;
            }
        }
        if (onlyZeroAndOne) {
            return 1;
        }
        // Must not be a multicast address
        if ((candidate[0] & 1) != 0) {
            return 1;
        }
        // Prefer globally unique address.
        if ((current[0] & 2) == 0) {
            if ((candidate[0] & 2) == 0) {
                // Both current and candidate are globally unique addresses.
                return 0;
            } else {
                // Only current is globally unique.
                return 1;
            }
        } else {
            if ((candidate[0] & 2) == 0) {
                // Only candidate is globally unique.
                return -1;
            } else {
                // Both current and candidate are non-unique.
                return 0;
            }
        }
    }

    /**
     * @return positive - current is better, 0 - cannot tell, negative - candidate is better
     */
    private static final int compareAddresses(InetAddress current, InetAddress candidate) {
        return scoreAddress(current) - scoreAddress(candidate);
    }

    private static final int scoreAddress(InetAddress addr) {
        if (addr.isAnyLocalAddress()) {
            return 0;
        }
        if (addr.isMulticastAddress()) {
            return 1;
        }
        if (addr.isLinkLocalAddress()) {
            return 2;
        }
        if (addr.isSiteLocalAddress()) {
            return 3;
        }

        return 4;
    }
    
    // pulled from http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
    public static final int jvmProcessId() {
        // Note: may fail in some JVM implementations
        // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
        try {
            final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            final int index = jvmName.indexOf('@');
    
            if (index < 1) {
                System.err.println("Could not get JVMPID");
                return RANDOM.nextInt(MAX_PID);
            }
            try {
                return Integer.parseInt(jvmName.substring(0, index)) % MAX_PID;
            } catch (NumberFormatException e) {
                System.err.println("Could not get JVMPID");
                e.printStackTrace();
                return RANDOM.nextInt(MAX_PID);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return RANDOM.nextInt(MAX_PID);
        }
    }
    
}

