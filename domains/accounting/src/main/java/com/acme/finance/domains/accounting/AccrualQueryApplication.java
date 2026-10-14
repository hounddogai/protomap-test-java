package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationDiscountAction;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.CountAccrualsRequest;
import com.acme.accounting.v1.CountAccrualsResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExportAccrualsRequest;
import com.acme.accounting.v1.ExportAccrualsResponse;
import com.acme.accounting.v1.FindAccrualsRequest;
import com.acme.accounting.v1.FindAccrualsResponse;
import com.acme.accounting.v1.LookupAccrualRequest;
import com.acme.accounting.v1.LookupAccrualResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PeekAccrualRequest;
import com.acme.accounting.v1.PeekAccrualResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResolveAccrualsRequest;
import com.acme.accounting.v1.ResolveAccrualsResponse;
import com.acme.accounting.v1.ScoreAllocationRequest;
import com.acme.accounting.v1.ScoreAllocationResponse;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SuggestAccrualsRequest;
import com.acme.accounting.v1.SuggestAccrualsResponse;
import com.acme.accounting.v1.SummarizeAccrualsRequest;
import com.acme.accounting.v1.SummarizeAccrualsResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.payroll.v1.ExplainPayRunDecisionRequest;
import com.acme.payroll.v1.ExplainPayRunDecisionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.sponsorships.v1.CalculateSponsorRequest;
import com.acme.sponsorships.v1.CalculateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about accruals, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AccrualQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(AccrualQueryApplication.class);

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final ChannelRegistry channels;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final AccrualQueryRequestMapper mapper;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final AccountingRepository repository;

    private final SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking;

    public AccrualQueryApplication(
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            ChannelRegistry channels,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            AccrualQueryRequestMapper mapper,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            AccountingRepository repository,
            SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking) {
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.accountingReconciliationValidationServiceBlocking = accountingReconciliationValidationServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.channels = channels;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.mapper = mapper;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.repository = repository;
        this.sponsorshipsSponsorCalculationServiceBlocking = sponsorshipsSponsorCalculationServiceBlocking;
    }

    /**
     * Returns an accrual.
     */
    public LookupAccrualResponse lookupAccrual(LookupAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "LookupAccrual");

        var outbound0 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainFixedAssetCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.FixedAssetCalculationService");
        var serviceName0 = "acme.assets.v1" + "." + "FixedAssetCalculationService";
        var methodName0 = "Exp" + "lainFixedAssetCalculation";
        var descriptor0 = MethodDescriptor.<ExplainFixedAssetCalculationRequest, ExplainFixedAssetCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainFixedAssetCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainFixedAssetCalculationResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ScoreAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAllocation(outbound1);
        return LookupAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .addLines(AccrualLine.newBuilder()
                                .setAmount(reply1.getAllocation().getDiscount().getAmount())
                                .setDescription(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists accruals a page at a time.
     */
    public BrowseAccrualsResponse browseAccruals(BrowseAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "BrowseAccruals");

        var outbound0 = PreviewAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setBlock(AllocationBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAllocationRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.AllocationValidationService");
        var selected0 = AllocationValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.previewAllocation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseAccruals", request.getPage().getPageToken());
        return BrowseAccrualsResponse.newBuilder()
                .addAccruals(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists accruals a page at a time.
     */
    public FindAccrualsResponse findAccruals(FindAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "FindAccruals");

        var outbound0 = ExplainPayRunDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPayRunDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunDecision(outbound0);

        var outbound1 = mapper.findAccrualsStage1(request, reply0);
        var reply1 = sponsorshipsSponsorCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateSponsor(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindAccruals", request.getPage().getPageToken());
        return FindAccrualsResponse.newBuilder()
                .addAccruals(Accrual.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(AllocationRef.newBuilder()
                                .setDisplayName(reply1.getSponsor().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts accruals by status.
     */
    public CountAccrualsResponse countAccruals(CountAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "CountAccruals");

        var outbound0 = ShareAuditTrailExportRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(ShareAuditTrailExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAuditTrailExport(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountAccruals", request.getPage().getPageToken());
        return CountAccrualsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several accruals at once.
     */
    public ResolveAccrualsResponse resolveAccruals(ResolveAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "ResolveAccruals");

        var outbound0 = EstimateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var reply0 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateReconciliation(outbound0);

        var outbound1 = JsonCodec.roundTrip(CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getReconciliation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), CreateUserRequest.class);
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createUser(outbound1);
        return ResolveAccrualsResponse.newBuilder()
                .addAccruals(Accrual.newBuilder()
                        .setCounterparty(AllocationRef.newBuilder()
                                .setDisplayName(reply0.getReconciliation().getCustomer().getDisplayName())
                                .build())
                        .setNumber(reply1.getUser().getNationalId().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Streams every matching accrual.
     */
    public ExportAccrualsResponse exportAccruals(ExportAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "ExportAccruals");

        var outbound0 = RpcSupport.unpack(Any.pack(PreviewAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAllocationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), PreviewAllocationRequest.class);
        var reply0 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAllocation(outbound0);

        var outbound1 = ValidateAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAllocation().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ValidateAccrualRequest, ValidateAccrualResponse> operation1 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateAccrual;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportAccruals", request.getFilter().getQuery());
        return ExportAccrualsResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(reply1.getAccrual().getCounterparty())
                        .addLines(AccrualLine.newBuilder()
                                .setDescription(reply0.getAllocation().getDescription())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes an accrual.
     */
    public SummarizeAccrualsResponse summarizeAccruals(SummarizeAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "SummarizeAccruals");

        var outbound0 = ScoreReconciliationRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreReconciliation(value)).orElseThrow();

        var outbound1 = PreviewAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getReconciliation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<PreviewAccrualRequest, PreviewAccrualResponse>() {
            @Override
            public PreviewAccrualResponse apply(PreviewAccrualRequest input) {
                return accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccrual(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeAccruals", request.getAccrualId());
        return SummarizeAccrualsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists accruals a page at a time.
     */
    public SuggestAccrualsResponse suggestAccruals(SuggestAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "SuggestAccruals");

        var outbound0 = ScoreAllocationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        Function<ScoreAllocationRequest, ScoreAllocationResponse> operation0 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreAllocation;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = (NormalizeAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setDiscount(AllocationDiscountAction.newBuilder()
                                .setPercent(reply0.getAllocation().getDiscount().getPercent())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAllocation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestAccruals", request.getPage().getPageToken());
        return SuggestAccrualsResponse.newBuilder()
                .addAccruals(Accrual.newBuilder()
                        .addLines(AccrualLine.newBuilder()
                                .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                .setDescription(reply0.getAllocation().getDescription())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns an accrual.
     */
    public PeekAccrualResponse peekAccrual(PeekAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "PeekAccrual");
        return PeekAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .build())
                .build();
    }
}
