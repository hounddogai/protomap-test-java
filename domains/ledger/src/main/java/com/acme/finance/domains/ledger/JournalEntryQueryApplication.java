package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LookupJournalEntryRequest;
import com.acme.ledger.v1.LookupJournalEntryResponse;
import com.acme.ledger.v1.PeekJournalEntryRequest;
import com.acme.ledger.v1.PeekJournalEntryResponse;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.metrics.v1.CreateMetricSeriesRequest;
import com.acme.metrics.v1.CreateMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about journal entries, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class JournalEntryQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryQueryApplication.class);

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final LedgerRepository repository;

    public JournalEntryQueryApplication(
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            LedgerRepository repository) {
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns a journal entry.
     */
    public LookupJournalEntryResponse lookupJournalEntry(LookupJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "LookupJournalEntry");
        return LookupJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Lists journal entries a page at a time.
     */
    public BrowseJournalEntriesResponse browseJournalEntries(BrowseJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "BrowseJournalEntries");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseJournalEntries", request.getPage().getPageToken());
        return BrowseJournalEntriesResponse.getDefaultInstance();
    }

    /**
     * Lists journal entries a page at a time.
     */
    public FindJournalEntriesResponse findJournalEntries(FindJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "FindJournalEntries");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindJournalEntries", request.getPage().getPageToken());
        return FindJournalEntriesResponse.getDefaultInstance();
    }

    /**
     * Returns several journal entries at once.
     */
    public ResolveJournalEntriesResponse resolveJournalEntries(ResolveJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "ResolveJournalEntries");
        return ResolveJournalEntriesResponse.getDefaultInstance();
    }

    /**
     * Streams every matching journal entry.
     */
    public ExportJournalEntriesResponse exportJournalEntries(ExportJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "ExportJournalEntries");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportJournalEntries", request.getFilter().getQuery());
        return ExportJournalEntriesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists journal entries a page at a time.
     */
    public SuggestJournalEntriesResponse suggestJournalEntries(SuggestJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "SuggestJournalEntries");

        var outbound0 = JsonCodec.roundTrip(CreateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), CreateMetricSeriesRequest.class);
        var reply0 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createMetricSeries(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestJournalEntries", request.getPage().getPageToken());
        return SuggestJournalEntriesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a journal entry.
     */
    public PeekJournalEntryResponse peekJournalEntry(PeekJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "PeekJournalEntry");
        return PeekJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }
}
