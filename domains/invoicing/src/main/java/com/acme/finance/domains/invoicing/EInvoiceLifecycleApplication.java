package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.ResolveBenefitPlansRequest;
import com.acme.benefits.v1.ResolveBenefitPlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentRouteAction;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ActivateEInvoiceRequest;
import com.acme.invoicing.v1.ActivateEInvoiceResponse;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.ExpireEInvoiceRequest;
import com.acme.invoicing.v1.ExpireEInvoiceResponse;
import com.acme.invoicing.v1.GetEInvoiceLifecycleRequest;
import com.acme.invoicing.v1.GetEInvoiceLifecycleResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.RecalculateEInvoicesRequest;
import com.acme.invoicing.v1.RecalculateEInvoicesResponse;
import com.acme.invoicing.v1.ReinstateEInvoiceRequest;
import com.acme.invoicing.v1.ReinstateEInvoiceResponse;
import com.acme.invoicing.v1.RenewEInvoiceRequest;
import com.acme.invoicing.v1.RenewEInvoiceResponse;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryRequest;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryResponse;
import com.acme.invoicing.v1.SuspendEInvoiceRequest;
import com.acme.invoicing.v1.SuspendEInvoiceResponse;
import com.acme.invoicing.v1.TerminateEInvoiceRequest;
import com.acme.invoicing.v1.TerminateEInvoiceResponse;
import com.acme.invoicing.v1.WatchInvoiceBatchesRequest;
import com.acme.invoicing.v1.WatchInvoiceBatchesResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
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
 * Moves an e invoice between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class EInvoiceLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceLifecycleApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceFutureStub featureflagsFeatureFlagValidationServiceFuture;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceBlockingStub invoicingInvoiceBatchServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking;

    private final EInvoiceLifecycleRequestMapper mapper;

    private final InvoicingRepository repository;

    private final ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    public EInvoiceLifecycleApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking,
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceFutureStub featureflagsFeatureFlagValidationServiceFuture,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceBlockingStub invoicingInvoiceBatchServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking,
            EInvoiceLifecycleRequestMapper mapper,
            InvoicingRepository repository,
            ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.benefitsBenefitPlanQueryServiceBlocking = benefitsBenefitPlanQueryServiceBlocking;
        this.channels = channels;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceFuture = featureflagsFeatureFlagValidationServiceFuture;
        this.invoicingInvoiceBatchServiceAsync = invoicingInvoiceBatchServiceAsync;
        this.invoicingInvoiceBatchServiceBlocking = invoicingInvoiceBatchServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.invoicingInvoiceTemplateCalculationServiceBlocking = invoicingInvoiceTemplateCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureApprovalServiceBlocking = riskExposureApprovalServiceBlocking;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
    }

    public record ReinstateEInvoiceStage2Requested(CancelControlWorkflowRequest request, CompletableFuture<CancelControlWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Activates an e invoice.
     */
    public ActivateEInvoiceResponse activateEInvoice(ActivateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "ActivateEInvoice");

        var outbound0 = WatchInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = invoicingInvoiceBatchServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchInvoiceBatches(outbound0);
        var reply0 = WatchInvoiceBatchesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(ResolveBenefitPlansRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveBenefitPlansRequest.class);
        var reply1 = benefitsBenefitPlanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBenefitPlans(outbound1);

        var outbound2 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input2 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getDetails().getNewOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addComments(DisposalComment.newBuilder()
                                .setAuthorEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input3 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateEInvoice", request.getDetails().getReasonCode());
        return ActivateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(ActivateEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Suspends an e invoice.
     */
    public SuspendEInvoiceResponse suspendEInvoice(SuspendEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "SuspendEInvoice");

        var outbound0 = BeanCopies.copy(SimulateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setBlock(CustomerSegmentBlockAction.newBuilder()
                                .setNotifyEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build(), SimulateCustomerSegmentRequest.class);
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(outbound0);

        var outbound1 = EstimateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCustomerSegment().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .setRoute(CustomerSegmentRouteAction.newBuilder()
                                .setTarget(switch (reply0.getCustomerSegment().getActionCase()) {
                                    case ROUTE -> reply0.getCustomerSegment().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCustomerSegment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendEInvoice", request.getDetails().getReasonCode());
        return SuspendEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(SuspendEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Reinstates an e invoice.
     */
    public ReinstateEInvoiceResponse reinstateEInvoice(ReinstateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "ReinstateEInvoice");

        var outbound0 = RecalculateEInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.invoicing.v1.EInvoiceCalculationService");
        var serviceName0 = "acme.invoicing.v1" + "." + "EInvoiceCalculationService";
        var methodName0 = "Rec" + "alculateEInvoices";
        var descriptor0 = MethodDescriptor.<RecalculateEInvoicesRequest, RecalculateEInvoicesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateEInvoicesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateEInvoicesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getEInvoice().getBillingAddress().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = CancelControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getEInvoice().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setDetails(CancelControlWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CancelControlWorkflowResponse>();
        events.publishEvent(new ReinstateEInvoiceStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateEInvoice", request.getDetails().getReasonCode());
        return ReinstateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(reply0.getEInvoice().getNotes())
                        .setNumber(reply0.getEInvoice().getNumber())
                        .build())
                .setOutcome(ReinstateEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates an e invoice.
     */
    public TerminateEInvoiceResponse terminateEInvoice(TerminateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "TerminateEInvoice");

        var outbound0 = QuoteInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceTemplate(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateEInvoice", request.getDetails().getReasonCode());
        return TerminateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply0.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNumber(reply0.getInvoiceTemplate().getNumber())
                        .build())
                .setOutcome(TerminateEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Renews an e invoice.
     */
    public RenewEInvoiceResponse renewEInvoice(RenewEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "RenewEInvoice");

        var outbound0 = CompareInvoiceTemplateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceTemplateOptions(value)).orElseThrow();

        var outbound1 = ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getInvoiceTemplate().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = featureflagsFeatureFlagValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateFeatureFlag(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewEInvoice", request.getDetails().getReasonCode());
        return RenewEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getInvoiceTemplate().getBillingAddress().getRegion())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(RenewEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires an e invoice.
     */
    public ExpireEInvoiceResponse expireEInvoice(ExpireEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "ExpireEInvoice");

        var outbound0 = BatchCreateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateInvoiceBatchesRequest.StreamHeader.newBuilder()
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateInvoiceBatchesResponse.getDefaultInstance());
        var input0 = invoicingInvoiceBatchServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateInvoiceBatches(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireEInvoice", request.getDetails().getReasonCode());
        return ExpireEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(ExpireEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public GetEInvoiceLifecycleResponse getEInvoiceLifecycle(GetEInvoiceLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetEInvoiceLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "GetEInvoiceLifecycle");
        return GetEInvoiceLifecycleResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .build();
    }

    /**
     * Schedules an e invoice.
     */
    public ScheduleEInvoiceExpiryResponse scheduleEInvoiceExpiry(ScheduleEInvoiceExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleEInvoiceExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = mapper.scheduleEInvoiceExpiryStage0(request);
        var reply0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findCustomers(outbound0);

        var outbound1 = (ListPendingExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingExposuresRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = riskExposureApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingExposures(outbound1);

        var outbound2 = CheckInvoiceBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckInvoiceBatchEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<CheckInvoiceBatchEligibilityRequest, CheckInvoiceBatchEligibilityResponse>() {
            @Override
            public CheckInvoiceBatchEligibilityResponse apply(CheckInvoiceBatchEligibilityRequest input) {
                return invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceBatchEligibility(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleEInvoiceExpiry", request.getEInvoice().getEInvoiceId());
        return ScheduleEInvoiceExpiryResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(request.getEInvoice().getBillingAddress())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(ScheduleEInvoiceExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
