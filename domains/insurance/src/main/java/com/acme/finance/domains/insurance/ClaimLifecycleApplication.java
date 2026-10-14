package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.LintAllocationsRequest;
import com.acme.accounting.v1.LintAllocationsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SummarizeCustomersRequest;
import com.acme.customers.v1.SummarizeCustomersResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateClaimRequest;
import com.acme.insurance.v1.ActivateClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ExpireClaimRequest;
import com.acme.insurance.v1.ExpireClaimResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.RecalculateUnderwritingsRequest;
import com.acme.insurance.v1.RecalculateUnderwritingsResponse;
import com.acme.insurance.v1.ReinstateClaimRequest;
import com.acme.insurance.v1.ReinstateClaimResponse;
import com.acme.insurance.v1.RenewClaimRequest;
import com.acme.insurance.v1.RenewClaimResponse;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.StreamCoverageMetricsRequest;
import com.acme.insurance.v1.StreamCoverageMetricsResponse;
import com.acme.insurance.v1.SuspendClaimRequest;
import com.acme.insurance.v1.SuspendClaimResponse;
import com.acme.insurance.v1.TerminateClaimRequest;
import com.acme.insurance.v1.TerminateClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionRequest;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionResponse;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a claim between the states of its lifecycle. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class ClaimLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimLifecycleApplication.class);

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceFutureStub insuranceClaimCalculationServiceFuture;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public ClaimLifecycleApplication(
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            ChannelRegistry channels,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            ApplicationEventPublisher events,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceFutureStub insuranceClaimCalculationServiceFuture,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking,
            InsuranceRepository repository,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.channels = channels;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.events = events;
        this.insuranceClaimCalculationServiceFuture = insuranceClaimCalculationServiceFuture;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.invoicingEInvoiceValidationServiceBlocking = invoicingEInvoiceValidationServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    public record SuspendClaimStage1Requested(ExplainEInvoiceDecisionRequest request, CompletableFuture<ExplainEInvoiceDecisionResponse> completion, io.grpc.Context context) {}

    public record RenewClaimStage0Requested(SummarizeCustomersRequest request, CompletableFuture<SummarizeCustomersResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a claim.
     */
    public ActivateClaimResponse activateClaim(ActivateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "ActivateClaim");

        var outbound0 = PreviewEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewEInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = invoicingEInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewEInvoice(outbound0);

        var outbound1 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getEInvoice().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = insuranceClaimCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateClaim", request.getDetails().getReasonCode());
        return ActivateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .setOutcome(ActivateClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Suspends a claim.
     */
    public SuspendClaimResponse suspendClaim(SuspendClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "SuspendClaim");

        var outbound0 = ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setDetails(ValidateCustomerSegmentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment", outbound0);
        var reply0 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCustomerSegment(command0.payload());

        var outbound1 = ExplainEInvoiceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainEInvoiceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ExplainEInvoiceDecisionResponse>();
        events.publishEvent(new SuspendClaimStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = StreamCoverageMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamCoverageMetricsRequest.Filter.newBuilder()
                        .setRegion(reply1.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamCoverageMetrics(outbound2);
        var reply2 = StreamCoverageMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = ValidateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getEInvoice().getAudit().getCreatedBy())
                                .setEtag(reply1.getEInvoice().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ValidateAssetTransferRequest, ValidateAssetTransferResponse> operation3 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateAssetTransfer;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendClaim", request.getDetails().getReasonCode());
        return SuspendClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .build())
                .setOutcome(SuspendClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a claim.
     */
    public ReinstateClaimResponse reinstateClaim(ReinstateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "ReinstateClaim");

        var builder0 = PushPremiumRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(PushPremiumRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setPremium(Premium.newBuilder()
                .setCounterparty(SupplierRef.newBuilder()
                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(outbound0);

        var outbound1 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getPremium().getBillingAddress().getRegion())
                                .build())
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPremium().getSettlementCase()) {
                                    case CARD -> reply0.getPremium().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getPremium().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateClaim", request.getDetails().getReasonCode());
        return ReinstateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setOutcome(ReinstateClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a claim.
     */
    public TerminateClaimResponse terminateClaim(TerminateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "TerminateClaim");

        var outbound0 = BeanCopies.copy(PushPremiumRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(PushPremiumRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build(), PushPremiumRequest.class);
        var reply0 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(outbound0);

        var outbound1 = CheckControlEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setDetails(CheckControlEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CheckControlEligibilityRequest, CheckControlEligibilityResponse>() {
            @Override
            public CheckControlEligibilityResponse apply(CheckControlEligibilityRequest input) {
                return complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkControlEligibility(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateClaim", request.getDetails().getReasonCode());
        return TerminateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(reply1.getControl().getPriority())
                        .build())
                .setOutcome(TerminateClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a claim.
     */
    public RenewClaimResponse renewClaim(RenewClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "RenewClaim");

        var outbound0 = SummarizeCustomersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeCustomersRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var completion0 = new CompletableFuture<SummarizeCustomersResponse>();
        events.publishEvent(new RenewClaimStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = SimulateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setBlock(CustomerSegmentBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(outbound1);

        var outbound2 = ResolveClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.ClaimQueryService");
        var serviceName2 = "acme.insurance.v1" + "." + "ClaimQueryService";
        var methodName2 = "Res" + "olveClaims";
        var descriptor2 = MethodDescriptor.<ResolveClaimsRequest, ResolveClaimsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveClaimsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveClaimsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewClaim", request.getDetails().getReasonCode());
        return RenewClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .setOutcome(RenewClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a claim.
     */
    public ExpireClaimResponse expireClaim(ExpireClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "ExpireClaim");

        var outbound0 = ScoreSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ScoreSupplierRatingRequest, ScoreSupplierRatingResponse>() {
            @Override
            public ScoreSupplierRatingResponse apply(ScoreSupplierRatingRequest input) {
                return suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSupplierRating(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = StreamCoverageMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamCoverageMetrics(outbound1);
        var reply1 = StreamCoverageMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = RecalculateUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateUnderwritingsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSupplierRating().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<RecalculateUnderwritingsRequest, RecalculateUnderwritingsResponse>() {
            @Override
            public RecalculateUnderwritingsResponse apply(RecalculateUnderwritingsRequest input) {
                return insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateUnderwritings(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = SuggestSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(reply0.getSupplierRating().getMetadataMap())
                        .setOwnerId(reply0.getSupplierRating().getCounterparty().getCustomerId())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<SuggestSearchIndexesRequest, SuggestSearchIndexesResponse> port3 = RpcPort.adapt(searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestSearchIndexes);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireClaim", request.getDetails().getReasonCode());
        return ExpireClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .setOutcome(ExpireClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules a claim.
     */
    public ScheduleClaimExpiryResponse scheduleClaimExpiry(ScheduleClaimExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleClaimExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = BeanCopies.copy(QuoteCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(request.getClaim().getRequester().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build(), QuoteCustomerRequest.class);
        var reply0 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomer(outbound0);

        var outbound1 = LintAllocationsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomer().getAudit().getCreatedBy())
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAllocationsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getCustomer().getContact().getName())
                                .setPreferredChannel(request.getClaim().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<LintAllocationsRequest, LintAllocationsResponse>() {
            @Override
            public LintAllocationsResponse apply(LintAllocationsRequest input) {
                return accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAllocations(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleClaimExpiry", request.getClaim().getClaimId());
        return ScheduleClaimExpiryResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getAllocation().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setOutcome(ScheduleClaimExpiryResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
