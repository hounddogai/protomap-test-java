package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.QuoteFixedAssetRequest;
import com.acme.assets.v1.QuoteFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.FindControlsRequest;
import com.acme.compliance.v1.FindControlsResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverCalculationServiceGrpc;
import com.acme.drivers.v1.EstimateDriverRequest;
import com.acme.drivers.v1.EstimateDriverResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.labeling.v1.CloneLabelingTaskRequest;
import com.acme.labeling.v1.CloneLabelingTaskResponse;
import com.acme.labeling.v1.LabelSetRef;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueDimensions;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a fixed asset, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class FixedAssetCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetCalculationApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final DriverCalculationServiceGrpc.DriverCalculationServiceBlockingStub driversDriverCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final AssetsRepository repository;

    private final VenueValidationServiceGrpc.VenueValidationServiceFutureStub venuesVenueValidationServiceFuture;

    public FixedAssetCalculationApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            DriverCalculationServiceGrpc.DriverCalculationServiceBlockingStub driversDriverCalculationServiceBlocking,
            ApplicationEventPublisher events,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            AssetsRepository repository,
            VenueValidationServiceGrpc.VenueValidationServiceFutureStub venuesVenueValidationServiceFuture) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.driversDriverCalculationServiceBlocking = driversDriverCalculationServiceBlocking;
        this.events = events;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.labelingLabelingTaskServiceBlocking = labelingLabelingTaskServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
        this.venuesVenueValidationServiceFuture = venuesVenueValidationServiceFuture;
    }

    public record EstimateFixedAssetStage0Requested(FindControlsRequest request, CompletableFuture<FindControlsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a fixed asset.
     */
    public CalculateFixedAssetResponse calculateFixedAsset(CalculateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = RecalculateAttestationsRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateAttestationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAttestations(outbound0);

        var outbound1 = EstimateDriverRequest.newBuilder()
                .setContext(request.getContext())
                .setDriver(Driver.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getFixedAsset().getAddress().getCountryCode())
                                .setRecipient(reply0.getAttestation().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> driversDriverCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDriver(value));
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateFixedAsset", request.getFixedAsset().getFixedAssetId());
        return CalculateFixedAssetResponse.newBuilder()
                .setBreakdown(CalculateFixedAssetResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getAttestation().getTotal())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setCapacity(request.getFixedAsset().getCapacity())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getDriver().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a fixed asset.
     */
    public EstimateFixedAssetResponse estimateFixedAsset(EstimateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = FindControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindControlsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<FindControlsResponse>();
        events.publishEvent(new EstimateFixedAssetStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ScoreRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setLocality(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRepayment(outbound1);

        var outbound2 = NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRepayment().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(value)).orElseThrow();

        var outbound3 = CloneLabelingTaskRequest.newBuilder()
                .setContext(request.getContext())
                .setLabelingTask(LabelingTask.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .setCustomer(LabelSetRef.newBuilder()
                                .setDisplayName(reply1.getRepayment().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply3 = labelingLabelingTaskServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLabelingTask(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateFixedAsset", request.getFixedAsset().getFixedAssetId());
        return EstimateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .setCode(request.getFixedAsset().getCode())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply3.getLabelingTask().getAssignee().getEmail())
                                .setUserId(reply2.getCoverage().getOwner().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a fixed asset.
     */
    public QuoteFixedAssetResponse quoteFixedAsset(QuoteFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = ValidateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                                .setRegion(request.getDetails().getDestination().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSanction(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteFixedAsset", request.getFixedAsset().getFixedAssetId());
        return QuoteFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setStatus(request.getFixedAsset().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates a fixed asset.
     */
    public RecalculateFixedAssetsResponse recalculateFixedAssets(RecalculateFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = NormalizeLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeLoanRequest, NormalizeLoanResponse> port0 = RpcPort.adapt(lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeLoan);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = PreviewVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setDimensions(VenueDimensions.newBuilder()
                                .setHeight(request.getFixedAsset().getDimensions().getHeight())
                                .build())
                        .build())
                .build();
        var pending1 = venuesVenueValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewVenue(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateFixedAssets", request.getFixedAsset().getFixedAssetId());
        return RecalculateFixedAssetsResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setCode(request.getFixedAsset().getCode())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(reply1.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getComponentId())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getVenue().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Explains a fixed asset.
     */
    public ExplainFixedAssetCalculationResponse explainFixedAssetCalculation(ExplainFixedAssetCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainFixedAssetCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(outbound0);

        var outbound1 = NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditEventValidationService");
        var serviceName1 = "acme.audit.v1" + "." + "AuditEventValidationService";
        var methodName1 = "Nor" + "malizeAuditEvent";
        var descriptor1 = MethodDescriptor.<NormalizeAuditEventRequest, NormalizeAuditEventResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeAuditEventRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeAuditEventResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = RevertRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertRetentionPolicyRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.audit.v1.RetentionPolicyHistoryService/RevertRetentionPolicy", outbound2);
        var reply2 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertRetentionPolicy(command2.payload());

        var outbound3 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var stream3 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound3);
        var reply3 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainFixedAssetCalculation", request.getFixedAsset().getFixedAssetId());
        return ExplainFixedAssetCalculationResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(request.getFixedAsset().getName())
                        .build())
                .setOutcome(ExplainFixedAssetCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public GetFixedAssetRatesResponse getFixedAssetRates(GetFixedAssetRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFixedAssetRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "GetFixedAssetRates");

        var outbound0 = SummarizeMetricSeriesesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            SummarizeMetricSeriesesResponse fetch(SummarizeMetricSeriesesRequest input) {
                return metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMetricSerieses(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetFixedAssetRatesResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
