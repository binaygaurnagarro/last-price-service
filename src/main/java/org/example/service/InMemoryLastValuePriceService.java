package org.example.service;

import org.example.entity.PriceRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe in-memory implementation Last Value Price Service.
 *
 */
public class InMemoryLastValuePriceService implements LastValuePriceService{

    private static final int MAX_CHUNK_SIZE = 1000;

    // committed data visible to consumers
    private final Map<String, PriceRecord> committed = new ConcurrentHashMap<>();

    // staging per batchId: each staging map stores the best latest asOf per id within that batch
    private final Map<String, Map<String, PriceRecord>> staging = new HashMap<>();

    // lock to protect staging map structure and to allow atomic commits
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * Indicate a new batch has started.
     *
     * @param batchId unique batch identifier
     */
    @Override
    public void startBatch(String batchId) {

        Objects.requireNonNull(batchId, "batchId");
        rwLock.writeLock().lock();
        try {
            staging.computeIfAbsent(batchId, k -> new HashMap<>());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Upload a chunk of up to 1000 records for a batch.
     *
     * @param batchId unique batch identifier
     * @param records list of records (size must be <= 1000)
     */
    @Override
    public void uploadChunk(String batchId, List<PriceRecord> records) {

        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(records, "records");
        if (records.size() > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException("chunk size exceeds " + MAX_CHUNK_SIZE);
        }
        rwLock.writeLock().lock();
        try {
            Map<String, PriceRecord> batchMap = staging.computeIfAbsent(batchId, k -> new HashMap<>());
            records.forEach(priceRecord->{
                String id= priceRecord.getId();
                PriceRecord existing= batchMap.get(id);
                // keep the record with the latest asOf within the same batch
                if (existing == null || priceRecord.getAsOf().isAfter(existing.getAsOf())) {
                    batchMap.put(id,priceRecord);
                }

            });

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Complete the batch: atomically publish all records in the batch so they become visible to consumers.
     *
     * @param batchId unique batch identifier
     */
    @Override
    public void completeBatch(String batchId) {

        Objects.requireNonNull(batchId, "batchId");
        // take write lock to block reads while we atomically apply the batch
        rwLock.writeLock().lock();
        try {
            Map<String, PriceRecord> batchMap = staging.remove(batchId);
            if (batchMap == null || batchMap.isEmpty()) {
                // nothing to commit; benign no-op
                return;
            }
            // Merge: for each id, update committed only if the batch record's asOf is strictly newer

            batchMap.forEach((id, priceRecord) -> committed.compute(id, (k, current) -> {
                if (current == null) return priceRecord;
                // respect asOf ordering
                return priceRecord.getAsOf().isAfter(current.getAsOf()) ? priceRecord : current;
            }));

        }finally {
            rwLock.writeLock().unlock();
        }

    }

    /**
     * Cancel the batch: discard staged records.
     *
     * @param batchId unique batch identifier
     */
    @Override
    public void cancelBatch(String batchId) {

        Objects.requireNonNull(batchId, "batchId");
        rwLock.writeLock().lock();
        try {
            staging.remove(batchId);
        } finally {
            rwLock.writeLock().unlock();
        }

    }

    /**
     * Obtain the last price record (by asOf) for an instrument id, if present.
     * Consumers must always see only committed records; staged records are not visible until batch completion.
     *
     * @param id instrument id
     * @return Optional of PriceRecord
     */
    @Override
    public Optional<PriceRecord> getLast(String id) {
        Objects.requireNonNull(id, "id");
        // reads should not see staging; only committed
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(committed.get(id));
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
