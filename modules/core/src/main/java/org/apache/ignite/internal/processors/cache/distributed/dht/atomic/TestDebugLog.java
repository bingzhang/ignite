/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;

/**
 * TODO
 if (!cacheMsg.partitionExchangeMessage() && !(cacheMsg instanceof GridDhtPartitionDemandMessage) && !(cacheMsg instanceof GridDhtPartitionSupplyMessage))
 TestDebugLog.addMessage("Message: " + cacheMsg);
 */
public class TestDebugLog {
    /** */
    private static final List<Object> msgs = Collections.synchronizedList(new ArrayList<>(1_000_000));

    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    private static final boolean disabled = false;

    static class Message {
        String thread = Thread.currentThread().getName();

        String msg;

        Object val;

        long ts = U.currentTimeMillis();

        public Message(String msg) {
            this.msg = msg;
        }

        public Message(String msg, Object val) {
            this.msg = msg;
            this.val = val;
        }

        public String toString() {
            return "Msg [thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) +
                ", msg=" + msg + ", val=" + val + ']';
        }
    }

    static class TxMessage extends Message {
        Object ver;
        Object val;

        public TxMessage(Object ver, Object val, String msg) {
            super(msg);

            this.ver = ver;
            this.val = val;
        }

        public String toString() {
            String s = "TxMsg [ver=" + ver +
                ", msg=" + msg +
                ", thread=" + thread +
                ", val=" + val +
                ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';

//            if (val instanceof Exception) {
//                System.out.println("Error for " + s);
//
//                ((Exception) val).printStackTrace(System.out);
//            }

            return s;
        }
    }

    static class PartMessage extends Message {
        private final int cacheId;
        private final int partId;
        private Object val;

        public PartMessage(int cacheId, int partId, Object val, String msg) {
            super(msg);

            this.cacheId = cacheId;
            this.partId = partId;
            this.val = val;
            this.msg = msg;
        }

        boolean match(int cacheId, int partId) {
            return this.cacheId == cacheId && this.partId == partId;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartMessage partKey = (PartMessage) o;

            if (cacheId != partKey.cacheId) return false;
            return partId == partKey.partId;

        }

        @Override public int hashCode() {
            int result = cacheId;
            result = 31 * result + partId;
            return result;
        }

        public String toString() {
            return "PartMessage [partId=" + partId +
                ", val=" + val +
                ", msg=" + msg +
                ", thread=" + thread +
                ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) +
                ", cacheId=" + cacheId + ']';
        }
    }

    static class EntryMessage extends Message {
        Object key;
        Object val;

        public EntryMessage(Object key, Object val, String msg) {
            super(msg);

            this.key = key;
            this.val = val;
        }

        public String toString() {
            //return "EntryMsg [key=" + key + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ", val=" + val + ']';

            String s = "EntryMsg [key=" + key +
                ", thread=" + thread +
                ", msg=" + msg +
                ", val=" + val +
                ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';

//            if (val instanceof Exception) {
//                System.out.println("Error for " + s);
//
//                ((Exception) val).printStackTrace(System.out);
//            }

            return s;
        }
    }

    static final boolean out = false;

    public static void startRoutineMsg(TcpDiscoveryAbstractMessage msg, Marshaller marsh, String txt) {
        if (msg instanceof TcpDiscoveryCustomEventMessage)
            startRoutineMsg((TcpDiscoveryCustomEventMessage)msg, marsh, txt);
    }

    public static void startRoutineMsg(TcpDiscoveryCustomEventMessage msg, Marshaller marsh, String txt) {
//        try {
//            CustomMessageWrapper msg0 = (CustomMessageWrapper)msg.message(marsh);
//
//            DiscoveryCustomMessage custMsg = msg0.delegate();
//
//            if (custMsg instanceof StartRoutineDiscoveryMessage) {
//                StartRoutineDiscoveryMessage startMsg = (StartRoutineDiscoveryMessage)custMsg;
//
//                addEntryMessage(startMsg.routineId(), "start msg", txt);
//            }
//            else if (custMsg instanceof StartRoutineAckDiscoveryMessage) {
//                StartRoutineAckDiscoveryMessage startMsg = (StartRoutineAckDiscoveryMessage)custMsg;
//
//                addEntryMessage(startMsg.routineId(), "start ack msg", txt);
//            }
//        }
//        catch (Throwable e) {
//            e.printStackTrace();
//
//            System.exit(55);
//        }
    }

    public static void addMessage(String msg) {
//        if (disabled)
//            return;
//
        msgs.add(new Message(msg));

        if (out)
            System.out.println(msg);
    }

    public static void addMessage(String msg, Object val) {
        if (disabled)
            return;

        msgs.add(new Message(msg, val));

        if (out)
            System.out.println(msg);
    }

    public static void addKeyMessage(CacheObject key, Object val, String msg) {
        addEntryMessage(key.value(null, false), val, msg);
    }

    public static void addQueueItemKeyMessage(KeyCacheObject key, Object val, String msg) {
//        Object q = key.value(null, false);
//
//        if (q instanceof GridCacheQueueItemKey)
//            addEntryMessage(((GridCacheQueueItemKey)q).index(), val, msg);
//        else
//            addEntryMessage(q, val, msg);
    }

    public static void addEntryMessage(Object key, Object val, String msg) {
        if (disabled)
            return;

        assert key != null;

        if (key instanceof CacheObject)
            key = ((CacheObject) key).value(null, false);

        EntryMessage msg0 = new EntryMessage(key, val, msg);

        msgs.add(msg0);

        if (out)
            System.out.println(msg0.toString());
    }

    public static void printMessages(String fileName) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            // msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0)
                    w.println(msg.toString());

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0)
                System.out.println(msg);
        }
    }

    public static void addPartMessage(int cacheId,
                                      int partId,
                                      Object val,
                                      String msg) {
        msgs.add(new PartMessage(cacheId, partId, val, msg));
    }

    public static void printKeyMessages(String fileName, Object key) {
        assert key != null;

        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                    continue;

                System.out.println(msg);
            }
        }
    }
    public static void printKeyAndPartMessages(String fileName, Object key, int partId, int cacheId) {
        //assert key != null;

        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (key != null) {
                        if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                            continue;
                    }

                    if (msg instanceof PartMessage) {
                        PartMessage pm = (PartMessage)msg;

                        if (pm.cacheId != cacheId || pm.partId != partId)
                            continue;
                    }

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
//            for (Object msg : msgs0) {
//                if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
//                    continue;
//
//                System.out.println(msg);
//            }
        }
    }

    public static void clear() {
        msgs.clear();
    }

    public static void main(String[] args) {
    }
}
