package com.mvp.module2.fusion.tools;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public class LogInternMemoryProbe {

    private static final int DEFAULT_EVENT_COUNT = 800_000;

    public static void main(String[] args) throws Exception {
        String mode = args.length >= 1 ? normalizeMode(args[0]) : "plain";
        boolean useIntern = "intern".equals(mode);
        int eventCount = args.length >= 2 ? parsePositiveInt(args[1], "eventCount") : DEFAULT_EVENT_COUNT;

        forceGc();
        long baselineBytes = usedHeapBytes();
        List<String> logBuffer = buildLogBuffer(eventCount, useIntern);
        forceGc();
        long afterBuildBytes = usedHeapBytes();

        long deltaBytes = Math.max(0L, afterBuildBytes - baselineBytes);
        int identityCount = countIdentity(logBuffer);
        int logicalValueCount = countLogicalValues(logBuffer);
        int guard = logBuffer.get(logBuffer.size() - 1).length();

        System.out.println("SCENARIO=simple-logging-message-dedup");
        System.out.println("MODE=" + mode);
        System.out.println("EVENT_COUNT=" + eventCount);
        System.out.println("HEAP_BASELINE_MB=" + formatMiB(baselineBytes));
        System.out.println("HEAP_AFTER_BUILD_MB=" + formatMiB(afterBuildBytes));
        System.out.println("HEAP_DELTA_MB=" + formatMiB(deltaBytes));
        System.out.println("IDENTITY_MESSAGE_COUNT=" + identityCount);
        System.out.println("LOGICAL_MESSAGE_COUNT=" + logicalValueCount);
        System.out.println("GUARD=" + guard);
    }

    private static List<String> buildLogBuffer(int eventCount, boolean useIntern) {
        List<String> buffer = new ArrayList<>(eventCount);
        for (int i = 0; i < eventCount; i += 1) {
            String module = new String(("module-" + (i % 97)).toCharArray());
            String level = new String(("level-" + (i % 5)).toCharArray());
            String action = new String(("action-" + (i % 7)).toCharArray());
            String message = "module=" + module + "|level=" + level + "|action=" + action;
            if (useIntern) {
                message = message.intern();
            }
            buffer.add(message);
        }
        return buffer;
    }

    private static int countIdentity(List<String> values) {
        IdentityHashMap<String, Boolean> refs = new IdentityHashMap<>();
        for (String value : values) {
            refs.put(value, Boolean.TRUE);
        }
        return refs.size();
    }

    private static int countLogicalValues(List<String> values) {
        Set<String> logical = new HashSet<>(values);
        return logical.size();
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 4; i += 1) {
            System.gc();
            Thread.sleep(120L);
        }
    }

    private static String formatMiB(long bytes) {
        double mib = bytes / (1024.0 * 1024.0);
        return String.format(Locale.ROOT, "%.2f", mib);
    }

    private static int parsePositiveInt(String raw, String argName) {
        Objects.requireNonNull(raw, argName + " is required");
        String normalized = raw.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(argName + " is blank");
        }
        int value;
        try {
            value = Integer.parseInt(normalized);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(argName + " is not a valid integer: " + raw, error);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(argName + " must be > 0");
        }
        return value;
    }

    private static String normalizeMode(String raw) {
        String normalized = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        if ("plain".equals(normalized) || "intern".equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("mode must be plain or intern");
    }

}
