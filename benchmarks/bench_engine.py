package com.enterprise.lsm.benchmark;

import com.enterprise.lsm.engine.EnterpriseLSMEngine;
import com.enterprise.lsm.engine.LSMConfig;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LSMEnterpriseMicrobenchmark {

    private static final int WARMUP_OPERATIONS = 50_000;
    private static final int TARGET_OPERATIONS = 200_000;
    private static final int KEY_SPACE = 1_000_000;
    private static final int PAYLOAD_SIZE = 256; // bytes
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws Exception {
        File dataDir = Files.createTempDirectory("lsm_bench_").toFile();
        dataDir.deleteOnExit();

        LSMConfig config = new LSMConfig.Builder()
                .withDataDirectory(dataDir)
                .withMemTableSizeThreshold(4 * 1024 * 1024)
                .withMaxLevels(4)
                .withBloomFilterFpp(0.01)
                .build();

        System.out.println("Initializing EnterpriseLSMEngine...");
        try (EnterpriseLSMEngine engine = new EnterpriseLSMEngine(config)) {
            
            byte[] payload = new byte[PAYLOAD_SIZE];
            new Random(42).nextBytes(payload);

            System.out.println("Preparing key-value dataset...");
            byte[][] keys = new byte[KEY_SPACE][];
            for (int i = 0; i < KEY_SPACE; i++) {
                keys[i] = String.format("key-%09d", i).getBytes(StandardCharsets.UTF_8);
            }

            // --- WARMUP PHASE ---
            System.out.println("Running warmup phase (" + WARMUP_OPERATIONS + " ops)...");
            executeBenchmark(engine, keys, payload, WARMUP_OPERATIONS, false);

            // --- WRITE BENCHMARK ---
            System.out.println("Running WRITE throughput and latency benchmark (" + TARGET_OPERATIONS + " ops)...");
            BenchmarkResult writeResult = executeBenchmark(engine, keys, payload, TARGET_OPERATIONS, true);

            // --- READ BENCHMARK ---
            System.out.println("Running READ throughput and latency benchmark (" + TARGET_OPERATIONS + " ops)...");
            BenchmarkResult readResult = executeBenchmark(engine, keys, payload, TARGET_OPERATIONS, false);

            // --- OUTPUT FORMATTED METRICS ---
            printResults(writeResult, readResult);
        }
    }

    private static BenchmarkResult executeBenchmark(
            EnterpriseLSMEngine engine, 
            byte[][] keys, 
            byte[] payload, 
            int operations, 
            boolean isWrite) throws InterruptedException {
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = operandLatches(THREAD_COUNT);

        int opsPerThread = operations / THREAD_COUNT;
        long[] allLatches = new long[operations];
        AtomicLong globalIndex = new AtomicLong(0);

        ThreadResult[] threadResults = new ThreadResult[THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long[] localLatches = new long[opsPerThread];
                    Random rand = new Random(42 + threadId);
                    
                    long startTime = System.nanoTime();
                    for (int i = 0; i < opsPerThread; i++) {
                        int keyIndex = rand.nextInt(KEY_SPACE);
                        byte[] key = keys[keyIndex];

                        long opStart = System.nanoTime();
                        if (isWrite) {
                            engine.put(key, payload);
                        } else {
                            engine.get(key);
                        }
                        long opEnd = System.nanoTime();

                        localLatches[i] = opEnd - opStart;
                    }
                    long endTime = System.nanoTime();

                    threadResults[threadId] = new ThreadResult(endTime - startTime, localLatches);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        long totalStartTime = System.nanoTime();
        startLatch.countDown();
        endLatch.await();
        long totalEndTime = System.nanoTime();
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long totalDurationNanos = totalEndTime - totalStartTime;

        // Aggregate latches
        int offset = 0;
        for (ThreadResult tr : threadResults) {
            System.arraycopy(tr.latches, 0, allLatches, offset, tr.latches.length);
            offset += tr.latches.length;
        }

        Arrays.sort(allLatches);

        double opsPerSec = (operations * 1_000_000_000.0) / totalDurationNanos;
        double p50 = allLatches[(int) (operations * 0.50)] / 1_000_000.0;
        double p99 = allLatches[(int) (operations * 0.99)] / 1_000_000.0;
        double p999 = allLatches[(int) (operations * 0.999)] / 1_000_000.0;

        return new BenchmarkResult(isWrite ? "WRITE" : "READ", operations, opsPerSec, p50, p99, p999);
    }

    private static CountDownLatch operandLatches(int count) {
        return new CountDownLatch(count);
    }

    private static void printResults(BenchmarkResult write, BenchmarkResult read) {
        String line = "+----------------+---------------+-------------------+-------------------+-------------------+-------------------+";
        System.out.println("\n");
        System.out.println("=== LSM ENTERPRISE ENGINE MICROBENCHMARK RESULTS ===");
        System.out.println(line);
        System.out.printf("| %-14s | %-13s | %-17s | %-17s | %-17s | %-17s |\n", 
                "Operation", "Total Ops", "Throughput", "p50 Latency", "p99 Latency", "p99.9 Latency");
        System.out.println(line);
        printRow(write);
        printRow(read);
        System.out.println(line);
        System.out.println();
    }

    private static void printRow(BenchmarkResult res) {
        System.out.printf("| %-14s | %-13d | %12.2f ops/s | %13.3f ms | %13.3f ms | %13.3f ms |\n",
                res.operation,
                res.operations,
                res.throughputOpsPerSec,
                res.p50LatencyMs,
                res.p99LatencyMs,
                res.p999LatencyMs);
    }

    private static class ThreadResult {
        final long durationNanos;
        final long[] latches;

        private ThreadResult(long durationNanos, long[] latches) {
            this.durationNanos = durationNanos;
            this.latches = latches;
        }
    }

    private static class BenchmarkResult {
        final String operation;
        final int operations;
        final double throughputOpsPerSec;
        final double p50LatencyMs;
        final double p99LatencyMs;
        final double p999LatencyMs;

        private BenchmarkResult(String operation, int operations, double throughputOpsPerSec, 
                                double p50LatencyMs, double p99LatencyMs, double p999LatencyMs) {
            this.operation = operation;
            this.operations = operations;
            this.throughputOpsPerSec = throughputOpsPerSec;
            this.p50LatencyMs = p50LatencyMs;
            this.p99LatencyMs = p99LatencyMs;
            this.p999LatencyMs = p999LatencyMs;
        }
    }
}