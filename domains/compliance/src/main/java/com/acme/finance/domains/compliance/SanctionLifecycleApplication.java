package com.acme.finance.domains.compliance;

import com.acme.appointments.v1.Appointment;
import com.acme.appointments.v1.AppointmentParticipant;
import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.CreateAppointmentRequest;
import com.acme.appointments.v1.CreateAppointmentResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ActivateSanctionRequest;
import com.acme.compliance.v1.ActivateSanctionResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.ExpireSanctionRequest;
import com.acme.compliance.v1.ExpireSanctionResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetSanctionLifecycleRequest;
import com.acme.compliance.v1.GetSanctionLifecycleResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.ReinstateSanctionRequest;
import com.acme.compliance.v1.ReinstateSanctionResponse;
import com.acme.compliance.v1.RenewSanctionRequest;
import com.acme.compliance.v1.RenewSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScheduleSanctionExpiryRequest;
import com.acme.compliance.v1.ScheduleSanctionExpiryResponse;
import com.acme.compliance.v1.SuspendSanctionRequest;
import com.acme.compliance.v1.SuspendSanctionResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.AdvanceLoanWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanWorkflowResponse;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RejectStressTestRequest;
import com.acme.risk.v1.RejectStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.travel.v1.SearchTripsRequest;
import com.acme.travel.v1.SearchTripsResponse;
import com.acme.travel.v1.TripServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a sanction between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class SanctionLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(SanctionLifecycleApplication.class);

    private final AppointmentServiceGrpc.AppointmentServiceBlockingStub appointmentsAppointmentServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceFutureStub lendingCollateralWorkflowServiceFuture;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final ComplianceRepository repository;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync;

    private final TripServiceGrpc.TripServiceBlockingStub travelTripServiceBlocking;

    public SanctionLifecycleApplication(
            AppointmentServiceGrpc.AppointmentServiceBlockingStub appointmentsAppointmentServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceFutureStub lendingCollateralWorkflowServiceFuture,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            ComplianceRepository repository,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking,
            SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync,
            TripServiceGrpc.TripServiceBlockingStub travelTripServiceBlocking) {
        this.appointmentsAppointmentServiceBlocking = appointmentsAppointmentServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.lendingCollateralWorkflowServiceFuture = lendingCollateralWorkflowServiceFuture;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.riskStressTestApprovalServiceBlocking = riskStressTestApprovalServiceBlocking;
        this.suppliersSupplierServiceAsync = suppliersSupplierServiceAsync;
        this.travelTripServiceBlocking = travelTripServiceBlocking;
    }

    /**
     * Activates a sanction.
     */
    public ActivateSanctionResponse activateSanction(ActivateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "ActivateSanction");

        var outbound0 = ListCollateralWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCollateralWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.CollateralWorkflowService/ListCollateralWorkflows", outbound0);
        var reply0 = lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCollateralWorkflows(command0.payload());

        var outbound1 = (SuggestDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build()).toBuilder().build();
        var reply1 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestDocuments(outbound1);

        var outbound2 = SearchTripsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchTripsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(travelTripServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchTrips(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = BatchCreateSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateSuppliersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateSuppliersResponse.getDefaultInstance());
        var input3 = suppliersSupplierServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateSuppliers(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateSanction", request.getDetails().getReasonCode());
        return ActivateSanctionResponse.newBuilder()
                .setOutcome(ActivateSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Suspends a sanction.
     */
    public SuspendSanctionResponse suspendSanction(SuspendSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "SuspendSanction");

        var outbound0 = JsonCodec.roundTrip(ExplainControlCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build(), ExplainControlCalculationRequest.class);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainControlCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendSanction", request.getDetails().getReasonCode());
        return SuspendSanctionResponse.newBuilder()
                .setOutcome(SuspendSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .addChildren(Sanction.newBuilder()
                                                .setName(reply0.getControl().getName())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Reinstates a sanction.
     */
    public ReinstateSanctionResponse reinstateSanction(ReinstateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "ReinstateSanction");

        var outbound0 = BeanCopies.copy(ExplainControlCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setDetails(ExplainControlCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ExplainControlCalculationRequest.class);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainControlCalculation(outbound0);

        var outbound1 = PreviewControlRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewControlRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewControlRequest, PreviewControlResponse> port1 = RpcPort.adapt(complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewControl);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setReason(switch (reply1.getControl().getActionCase()) {
                    case BLOCK -> reply1.getControl().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply2 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeRetentionPolicyUpdates(outbound2);

        var outbound3 = PullRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(switch (reply2.getRetentionPolicy().getActionCase()) {
                                    case BLOCK -> reply2.getRetentionPolicy().getBlock().getNotifyEmail();
                                    default -> "";
                                })
                                .setReason(switch (reply1.getControl().getActionCase()) {
                                    case BLOCK -> reply1.getControl().getBlock().getReason();
                                    default -> "";
                                })
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullRetentionPolicies(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateSanction", request.getDetails().getReasonCode());
        return ReinstateSanctionResponse.newBuilder()
                .setOutcome(ReinstateSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply3.getRetentionPolicy().getOwner().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(reply2.getRetentionPolicy().getOwner().getEmail())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Renews a sanction.
     */
    public RenewSanctionResponse renewSanction(RenewSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "RenewSanction");

        var outbound0 = BeanCopies.copy(RejectStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectStressTestRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build(), RejectStressTestRequest.class);
        var reply0 = riskStressTestApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectStressTest(outbound0);

        var outbound1 = CancelCollateralWorkflowRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getStressTest().getBillingAddress().getLocality())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = lendingCollateralWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).cancelCollateralWorkflow(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewSanction", request.getDetails().getReasonCode());
        return RenewSanctionResponse.newBuilder()
                .setOutcome(RenewSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .addChildren(Sanction.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(reply0.getStressTest().getBillingAddress().getLinesList())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Expires a sanction.
     */
    public ExpireSanctionResponse expireSanction(ExpireSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "ExpireSanction");

        var outbound0 = AdvanceLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceLoanWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.LoanWorkflowService/AdvanceLoanWorkflow", outbound0);
        var reply0 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceLoanWorkflow(command0.payload());

        var outbound1 = RejectRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(reply0.getLoan().getAudit().getEtag())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNewOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRiskRegister(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireSanction", request.getDetails().getReasonCode());
        return ExpireSanctionResponse.newBuilder()
                .setOutcome(ExpireSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Returns a sanction.
     */
    public GetSanctionLifecycleResponse getSanctionLifecycle(GetSanctionLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSanctionLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "GetSanctionLifecycle");
        return GetSanctionLifecycleResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Schedules a sanction.
     */
    public ScheduleSanctionExpiryResponse scheduleSanctionExpiry(ScheduleSanctionExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleSanctionExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = CreateAppointmentRequest.newBuilder()
                .setAppointment(Appointment.newBuilder()
                        .addParticipants(AppointmentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            CreateAppointmentResponse fetch(CreateAppointmentRequest input) {
                return appointmentsAppointmentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAppointment(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = AdvanceRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAppointment().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceRiskScenarioWorkflow(value));
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleSanctionExpiry", request.getSanction().getSanctionId());
        return ScheduleSanctionExpiryResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getRiskScenario().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getAppointment().getOrganizer().getDisplayName())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }
}
