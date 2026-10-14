package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ActivateReconciliationRequest;
import com.acme.accounting.v1.ActivateReconciliationResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.ExpireReconciliationRequest;
import com.acme.accounting.v1.ExpireReconciliationResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.GetReconciliationLifecycleRequest;
import com.acme.accounting.v1.GetReconciliationLifecycleResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.RenewReconciliationRequest;
import com.acme.accounting.v1.RenewReconciliationResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.SuspendReconciliationRequest;
import com.acme.accounting.v1.SuspendReconciliationResponse;
import com.acme.accounting.v1.TerminateReconciliationRequest;
import com.acme.accounting.v1.TerminateReconciliationResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payments.v1.PaymentQueryServiceGrpc;
import com.acme.payments.v1.SuggestPaymentsRequest;
import com.acme.payments.v1.SuggestPaymentsResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a reconciliation between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ReconciliationLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationLifecycleApplication.class);

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final PaymentQueryServiceGrpc.PaymentQueryServiceBlockingStub paymentsPaymentQueryServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final AccountingRepository repository;

    public ReconciliationLifecycleApplication(
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking,
            ApplicationEventPublisher events,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            PaymentQueryServiceGrpc.PaymentQueryServiceBlockingStub paymentsPaymentQueryServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            AccountingRepository repository) {
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.accountingChartOfAccountsQueryServiceBlocking = accountingChartOfAccountsQueryServiceBlocking;
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.accountingReconciliationValidationServiceBlocking = accountingReconciliationValidationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseWorkflowServiceBlocking = complianceComplianceCaseWorkflowServiceBlocking;
        this.events = events;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.paymentsPaymentQueryServiceBlocking = paymentsPaymentQueryServiceBlocking;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
    }

    public record RenewReconciliationStage0Requested(ScheduleMileageClaimExpiryRequest request, CompletableFuture<ScheduleMileageClaimExpiryResponse> completion, io.grpc.Context context) {}

    public record ExpireReconciliationStage1Requested(AdvanceExposureWorkflowRequest request, CompletableFuture<AdvanceExposureWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a reconciliation.
     */
    public ActivateReconciliationResponse activateReconciliation(ActivateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "ActivateReconciliation");

        var outbound0 = CheckChartOfAccountsEligibilityRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkChartOfAccountsEligibility(outbound0);

        var outbound1 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreatePurchaseRequisitionsResponse.getDefaultInstance());
        var input1 = procurementPurchaseRequisitionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePurchaseRequisitions(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(request.getDetails().getNewOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setAuthorEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateReconciliation", request.getDetails().getReasonCode());
        return ActivateReconciliationResponse.newBuilder()
                .setOutcome(ActivateReconciliationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }

    /**
     * Suspends a reconciliation.
     */
    public SuspendReconciliationResponse suspendReconciliation(SuspendReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "SuspendReconciliation");

        var outbound0 = CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setBlock(AllocationBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CompareAllocationOptionsRequest, CompareAllocationOptionsResponse> port0 = RpcPort.adapt(accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareAllocationOptions);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = PauseComplianceCaseWorkflowRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PauseComplianceCaseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.compliance.v1.ComplianceCaseWorkflowService/PauseComplianceCaseWorkflow", outbound1);
        var reply1 = complianceComplianceCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseComplianceCaseWorkflow(command1.payload());

        var outbound2 = SuggestPaymentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestPaymentsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getComplianceCase().getAudit().getUpdatedAt())
                        .setOwnerId(switch (reply1.getComplianceCase().getOriginCase()) {
                            case CUSTOMER -> reply1.getComplianceCase().getCustomer().getAttestationId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = paymentsPaymentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestPayments(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendReconciliation", request.getDetails().getReasonCode());
        return SuspendReconciliationResponse.newBuilder()
                .setOutcome(SuspendReconciliationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a reconciliation.
     */
    public TerminateReconciliationResponse terminateReconciliation(TerminateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "TerminateReconciliation");

        var outbound0 = ValidateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateReconciliationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.ReconciliationValidationService");
        var serviceName0 = "acme.accounting.v1" + "." + "ReconciliationValidationService";
        var methodName0 = "Val" + "idateReconciliation";
        var descriptor0 = MethodDescriptor.<ValidateReconciliationRequest, ValidateReconciliationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidateReconciliationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidateReconciliationResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateReconciliation", request.getDetails().getReasonCode());
        return TerminateReconciliationResponse.newBuilder()
                .setOutcome(TerminateReconciliationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setStatus(reply0.getReconciliation().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a reconciliation.
     */
    public RenewReconciliationResponse renewReconciliation(RenewReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "RenewReconciliation");

        var outbound0 = ScheduleMileageClaimExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleMileageClaimExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ScheduleMileageClaimExpiryResponse>();
        events.publishEvent(new RenewReconciliationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = (CalculateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getMileageClaim().getBillingAddress().getCountryCode())
                                .setRecipient(reply0.getMileageClaim().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAllocation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewReconciliation", request.getDetails().getReasonCode());
        return RenewReconciliationResponse.newBuilder()
                .setOutcome(RenewReconciliationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Expires a reconciliation.
     */
    public ExpireReconciliationResponse expireReconciliation(ExpireReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "ExpireReconciliation");

        var outbound0 = JsonCodec.roundTrip(FindChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindChartOfAccountsesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build(), FindChartOfAccountsesRequest.class);
        var reply0 = accountingChartOfAccountsQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findChartOfAccountses(outbound0);

        var outbound1 = AdvanceExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<AdvanceExposureWorkflowResponse>();
        events.publishEvent(new ExpireReconciliationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = PreviewReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewReconciliationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(reply1.getExposure().getContact().getPhone())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getExposure().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewReconciliationRequest, PreviewReconciliationResponse> port2 = RpcPort.adapt(accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewReconciliation);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = ExplainAllocationDecisionRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getReconciliation().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getExposure().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply2.getReconciliation().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAllocationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAllocationDecision(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireReconciliation", request.getDetails().getReasonCode());
        return ExpireReconciliationResponse.newBuilder()
                .setOutcome(ExpireReconciliationResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .addComments(ReconciliationComment.newBuilder()
                                .setCreatedAt(reply2.getReconciliation().getAudit().getCreatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a reconciliation.
     */
    public GetReconciliationLifecycleResponse getReconciliationLifecycle(GetReconciliationLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReconciliationLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "GetReconciliationLifecycle");

        var outbound0 = ExplainReconciliationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainReconciliationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReconciliationId(request.getReconciliationId())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainReconciliationCalculation(value)).orElseThrow();

        var outbound1 = ValidateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getReconciliation().getAudit().getCreatedBy())
                                .setEtag(reply0.getReconciliation().getAudit().getEtag())
                                .setUpdatedBy(reply0.getReconciliation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAllocationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getReconciliation().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAllocation(value)))
                .collect(Collectors.toList()).getFirst();
        return GetReconciliationLifecycleResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setPriority(reply1.getAllocation().getPriority())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Schedules a reconciliation.
     */
    public ScheduleReconciliationExpiryResponse scheduleReconciliationExpiry(ScheduleReconciliationExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleReconciliationExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = PreviewChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getReconciliation().getAudit().getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getReconciliation().getAssignee().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewChartOfAccounts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleReconciliationExpiry", request.getReconciliation().getReconciliationId());
        return ScheduleReconciliationExpiryResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setStatus(request.getReconciliation().getStatus())
                        .build())
                .build();
    }
}
