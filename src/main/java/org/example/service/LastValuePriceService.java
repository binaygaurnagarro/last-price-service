package org.example.service;

import org.example.entity.PriceRecord;

import java.util.List;
import java.util.Optional;

/**
 * Last Value Price Service API for producers and consumers.
 */
public interface LastValuePriceService {

    /**
     * Indicate a new batch has started.
     *
     * @param batchId unique batch identifier
     */
    void startBatch(String batchId);

    /**
     * Upload a chunk of up to 1000 records for a batch.
     *
     * @param batchId unique batch identifier
     * @param records list of records (size must be <= 1000)
     */
    void uploadChunk(String batchId, List<PriceRecord> records);

    /**
     * Complete the batch: atomically publish all records in the batch so they become visible to consumers.
     *
     * @param batchId unique batch identifier
     */
    void completeBatch(String batchId);

    /**
     * Cancel the batch: discard staged records.
     *
     * @param batchId unique batch identifier
     */
    void cancelBatch(String batchId);

    /**
     * Obtain the last price record (by asOf) for an instrument id, if present.
     * Consumers must always see only committed records; staged records are not visible until batch completion.
     *
     * @param id instrument id
     * @return Optional of PriceRecord
     */
    Optional<PriceRecord> getLast(String id);
}
