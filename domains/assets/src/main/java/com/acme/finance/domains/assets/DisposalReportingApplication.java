package com.acme.finance.domains.assets;

import com.acme.assets.v1.CountDisposalTotalsRequest;
import com.acme.assets.v1.CountDisposalTotalsResponse;
import com.acme.assets.v1.DeleteDisposalReportRequest;
import com.acme.assets.v1.DeleteDisposalReportResponse;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FetchDisposalReportRequest;
import com.acme.assets.v1.FetchDisposalReportResponse;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about disposals. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class DisposalReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalReportingApplication.class);

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final AssetsRepository repository;

    public DisposalReportingApplication(
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            AssetsRepository repository) {
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.repository = repository;
    }

    public record ShareDisposalReportStage0Requested(ValidateVenueRequest request, CompletableFuture<ValidateVenueResponse> completion, io.grpc.Context context) {}

    public record DeleteDisposalReportStage0Requested(ExplainVenueDecisionRequest request, CompletableFuture<ExplainVenueDecisionResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a disposal.
     */
    public GenerateDisposalReportResponse generateDisposalReport(GenerateDisposalReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateDisposalReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GenerateDisposalReport");

        var outbound0 = NormalizeFeatureFlagRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NormalizeFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setBlock(FeatureFlagBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag", outbound0);
        var reply0 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeFeatureFlag(command0.payload());

        var outbound1 = PreviewVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFeatureFlag().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getFeatureFlag().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.venues.v1.VenueValidationService");
        var serviceName1 = "acme.venues.v1" + "." + "VenueValidationService";
        var methodName1 = "Pre" + "viewVenue";
        var descriptor1 = MethodDescriptor.<PreviewVenueRequest, PreviewVenueResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewVenueRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewVenueResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateDisposalReport", request.getDisposalId());
        return GenerateDisposalReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public FetchDisposalReportResponse fetchDisposalReport(FetchDisposalReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchDisposalReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "FetchDisposalReport");

        var outbound0 = RpcSupport.unpack(Any.pack(SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .build())
                .build()), SuggestAuditEventsRequest.class);
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(outbound0);
        return FetchDisposalReportResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a disposal.
     */
    public ScheduleDisposalReportResponse scheduleDisposalReport(ScheduleDisposalReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDisposalReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "ScheduleDisposalReport");

        var outbound0 = NormalizeJournalEntryRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<NormalizeJournalEntryRequest, NormalizeJournalEntryResponse>() {
            @Override
            public NormalizeJournalEntryResponse apply(NormalizeJournalEntryRequest input) {
                return ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeJournalEntry(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDisposalReport", request.getDisposalId());
        return ScheduleDisposalReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Streams the changes of disposals as they happen.
     */
    public StreamDisposalMetricsResponse streamDisposalMetrics(StreamDisposalMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamDisposalMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getContext().getTraceId(), "StreamDisposalMetrics");

        var outbound0 = ValidateDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateDisposalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDisposal(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = StreamDepreciationMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getDisposal().getRequester().getEmail())
                        .setName(reply0.getDisposal().getRequester().getName())
                        .setPreferredChannel(reply0.getDisposal().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream1 = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDepreciationMetrics(outbound1);
        var reply1 = StreamDepreciationMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamDisposalMetrics", request.getFilter().getQuery());
        return StreamDisposalMetricsResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(reply0.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists disposals a page at a time.
     */
    public ListDisposalReportsResponse listDisposalReports(ListDisposalReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDisposalReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getContext().getTraceId(), "ListDisposalReports");

        var outbound0 = CountAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.audit.v1.AuditEventQueryService/CountAuditEvents", outbound0);
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAuditEvents(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDisposalReports", request.getPage().getPageToken());
        return ListDisposalReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Shares a disposal.
     */
    public ShareDisposalReportResponse shareDisposalReport(ShareDisposalReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareDisposalReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "ShareDisposalReport");

        var outbound0 = ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ValidateVenueResponse>();
        events.publishEvent(new ShareDisposalReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CommentWriteOffApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentWriteOffApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply0.getVenue().getOwner().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getVenue().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentWriteOffApproval(outbound1);

        var outbound2 = ShareDepreciationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareDepreciationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getVenue().getOwner().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDepreciationReport(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = GetDisposalSyncStateRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposalId(request.getDisposalId())
                .build();
        var channel3 = channels.lookup("acme.assets.v1.DisposalSyncService");
        var selected3 = DisposalSyncServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.getDisposalSyncState(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareDisposalReport", request.getDetails().getRecipient().getEmail());
        return ShareDisposalReportResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getVenue().getOwner().getDisplayName())
                                .build())
                        .setDisposalId(reply3.getDisposal().getDisposalId())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .setOutcome(ShareDisposalReportResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts disposals by status.
     */
    public CountDisposalTotalsResponse countDisposalTotals(CountDisposalTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountDisposalTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getContext().getTraceId(), "CountDisposalTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountDisposalTotals", request.getPage().getPageToken());
        return CountDisposalTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes a disposal.
     */
    public DeleteDisposalReportResponse deleteDisposalReport(DeleteDisposalReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteDisposalReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "DeleteDisposalReport");

        var outbound0 = ExplainVenueDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ExplainVenueDecisionResponse>();
        events.publishEvent(new DeleteDisposalReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteDisposalReport", request.getDisposalId());
        return DeleteDisposalReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
