package org.example.service;

import org.example.entity.PriceRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests
 */
public class InMemoryLastValuePriceServiceTest {

    @Test
    public void testBasicFlow() {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String batch = "batch1";

        svc.startBatch(batch);

        PriceRecord r1 = new PriceRecord("A", Instant.parse("2026-01-11T19:00:00Z"), Map.of("price", 100));
        PriceRecord r2 = new PriceRecord("B", Instant.parse("2026-01-11T20:00:00Z"), Map.of("price", 200));
        svc.uploadChunk(batch, List.of(r1, r2));

        // before completion, consumers see nothing
        assertTrue(svc.getLast("A").isEmpty());
        assertTrue(svc.getLast("B").isEmpty());

        svc.completeBatch(batch);

        assertEquals(100, svc.getLast("A").get().getPayload().get("price"));
        assertEquals(200, svc.getLast("B").get().getPayload().get("price"));
    }

    @Test
    public void testChunkReplacesWithinBatch() {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String batch = "batch2";

        svc.uploadChunk(batch, List.of(
                new PriceRecord("X", Instant.parse("2026-01-11T19:00:00Z"), Map.of("p", 1))
        ));
        svc.uploadChunk(batch, List.of(
                new PriceRecord("X", Instant.parse("2026-01-11T20:00:00Z"), Map.of("p", 2))
        ));

        svc.completeBatch(batch);
        assertEquals(2, svc.getLast("X").get().getPayload().get("p"));
    }

    @Test
    public void testCancelBatch() {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String batch = "batch-cancel";

        svc.startBatch(batch);
        svc.uploadChunk(batch, List.of(new PriceRecord("Z", Instant.now(), Map.of("p", 9))));
        svc.cancelBatch(batch);
        svc.completeBatch(batch);
        assertTrue(svc.getLast("Z").isEmpty());
    }

    @Test
    public void testOutOfOrderCompleteWithoutStart() {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String batch = "batch-no-start";

        // uploading without start should implicitly create staging
        svc.uploadChunk(batch, List.of(new PriceRecord("O", Instant.parse("2026-01-11T00:00:00Z"), Map.of("v", 5))));
        svc.completeBatch(batch);
        assertEquals(5, svc.getLast("O").get().getPayload().get("v"));
    }

    @Test
    public void testOlderRecordNotOverwriteNewer() {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String b1 = "b1";
        String b2 = "b2";

        svc.uploadChunk(b1, List.of(new PriceRecord("K", Instant.parse("2026-01-02T10:00:00Z"), Map.of("v", 10))));
        svc.completeBatch(b1);

        // a later batch with older asOf should not overwrite
        svc.uploadChunk(b2, List.of(new PriceRecord("K", Instant.parse("2026-01-01T10:00:00Z"), Map.of("v", 1))));
        svc.uploadChunk(b2, List.of(new PriceRecord("K", Instant.parse("2026-01-01T10:00:00Z"), Map.of("v", 1))));
        svc.completeBatch(b2);

        assertEquals(10, svc.getLast("K").get().getPayload().get("v"));
    }

    @Test
    public void testConcurrentReadsWhileCommitting() throws Exception {
        LastValuePriceService svc = new InMemoryLastValuePriceService();
        String batch = "concurrent";

        // prepopulate
        svc.uploadChunk("pre", List.of(new PriceRecord("C", Instant.parse("2026-01-11T01:00:00Z"), Map.of("v", 1))));
        svc.completeBatch("pre");

        // prepare a large batch for commit (simulate many records)
        List<PriceRecord> records = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            records.add(new PriceRecord("C", Instant.parse("2026-01-11T02:00:00Z"), Map.of("v", 2)));
        }
        svc.uploadChunk(batch, records);

        ExecutorService ex = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(1);
        Future<Boolean> reader = ex.submit(() -> {
            latch.await();
            // reader should either see the older value (1) or the newer value (2), but never partial/inconsistent state
            Optional<PriceRecord> r = svc.getLast("C");
            return r.isPresent() && (r.get().getPayload().get("v").equals(1) || r.get().getPayload().get("v").equals(2));
        });

        Future<Void> writer = ex.submit(() -> {
            latch.countDown();
            svc.completeBatch(batch);
            return null;
        });

        assertTrue(reader.get(5, TimeUnit.SECONDS));
        writer.get(5, TimeUnit.SECONDS);
        ex.shutdownNow();
    }
}
