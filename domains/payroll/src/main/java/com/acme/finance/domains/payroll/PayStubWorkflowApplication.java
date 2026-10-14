package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.banking.v1.CommentMandateApprovalRequest;
import com.acme.banking.v1.CommentMandateApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.payroll.v1.AdvancePayStubWorkflowRequest;
import com.acme.payroll.v1.AdvancePayStubWorkflowResponse;
import com.acme.payroll.v1.CancelPayStubWorkflowRequest;
import com.acme.payroll.v1.CancelPayStubWorkflowResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.InspectPayStubWorkflowRequest;
import com.acme.payroll.v1.InspectPayStubWorkflowResponse;
import com.acme.payroll.v1.LookupPayStubRequest;
import com.acme.payroll.v1.LookupPayStubResponse;
import com.acme.payroll.v1.PausePayStubWorkflowRequest;
import com.acme.payroll.v1.PausePayStubWorkflowResponse;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.ResumePayStubWorkflowRequest;
import com.acme.payroll.v1.ResumePayStubWorkflowResponse;
import com.acme.payroll.v1.RetryPayStubWorkflowRequest;
import com.acme.payroll.v1.RetryPayStubWorkflowResponse;
import com.acme.payroll.v1.ScheduleDeductionReportRequest;
import com.acme.payroll.v1.ScheduleDeductionReportResponse;
import com.acme.payroll.v1.SignalPayStubWorkflowRequest;
import com.acme.payroll.v1.SignalPayStubWorkflowResponse;
import com.acme.payroll.v1.StartPayStubWorkflowRequest;
import com.acme.payroll.v1.StartPayStubWorkflowResponse;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.tours.v1.CreateTourRequest;
import com.acme.tours.v1.CreateTourResponse;
import com.acme.tours.v1.Tour;
import com.acme.tours.v1.TourParticipant;
import com.acme.tours.v1.TourServiceGrpc;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a pay stub from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayStubWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubWorkflowApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking;

    private final PayStubWorkflowRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking;

    private final PayrollRepository repository;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    private final TourServiceGrpc.TourServiceBlockingStub toursTourServiceBlocking;

    public PayStubWorkflowApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking,
            PayStubWorkflowRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking,
            PayrollRepository repository,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TourServiceGrpc.TourServiceBlockingStub toursTourServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.contractsRenewalWorkflowServiceBlocking = contractsRenewalWorkflowServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollDeductionReportingServiceBlocking = payrollDeductionReportingServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.procurementPurchaseRequisitionWorkflowServiceBlocking = procurementPurchaseRequisitionWorkflowServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.toursTourServiceBlocking = toursTourServiceBlocking;
    }

    /**
     * Starts a pay stub.
     */
    public StartPayStubWorkflowResponse startPayStubWorkflow(StartPayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = CreateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayStub().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        Function<CreateNotificationRequest, CreateNotificationResponse> operation0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createNotification;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CreateTourRequest.newBuilder()
                .setContext(request.getContext())
                .setTour(Tour.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getPayStub().getBillingAddress().getRegion())
                                .build())
                        .addParticipants(TourParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply0.getNotification().getRequester().getPhone())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<CreateTourRequest, CreateTourResponse> operation1 = toursTourServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createTour;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPayStubWorkflow", request.getPayStub().getPayStubId());
        return StartPayStubWorkflowResponse.newBuilder()
                .setOutcome(StartPayStubWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getTour().getLocation().getPostalCode())
                                .build())
                        .putAllMetadata(request.getPayStub().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a pay stub.
     */
    public AdvancePayStubWorkflowResponse advancePayStubWorkflow(AdvancePayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var futures0 = request.getPayStub().getLinesList().stream()
                .map(element -> RetryRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .putAllAttributes(element.getAttributesMap())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryRiskRegisterWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RetryRiskRegisterWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getPayStub().getAttachmentsList().stream()
                .map(element -> AdvancePurchaseRequisitionWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePurchaseRequisitionWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? AdvancePurchaseRequisitionWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = LookupPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStubId(request.getPayStubId())
                .build();
        RpcPort<LookupPayStubRequest, LookupPayStubResponse> port2 = RpcPort.adapt(payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lookupPayStub);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePayStubWorkflow", request.getPayStub().getPayStubId());
        return AdvancePayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getPayStub().getCounterparty())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Cancels a pay stub.
     */
    public CancelPayStubWorkflowResponse cancelPayStubWorkflow(CancelPayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = ScheduleDeductionReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDeductionId(request.getPayStub().getCounterparty().getDeductionId())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> payrollDeductionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleDeductionReport(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPayStubWorkflow", request.getPayStub().getPayStubId());
        return CancelPayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a pay stub.
     */
    public RetryPayStubWorkflowResponse retryPayStubWorkflow(RetryPayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryPayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = ReinstateDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayStub().getCounterparty().getDeductionId())
                .setDetails(ReinstateDeductionRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPayStub().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateDeduction(outbound0), rpcExecutor);

        var outbound1 = CheckPayRunEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPayRunEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPayRunEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryPayStubWorkflow", request.getPayStub().getPayStubId());
        return RetryPayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(reply1.getPayRun().getBillingAddress())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(reply0.getDeduction().getDeductionId())
                                .build())
                        .setIssuedAt(request.getPayStub().getIssuedAt())
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a pay stub.
     */
    public PausePayStubWorkflowResponse pausePayStubWorkflow(PausePayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PausePayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = mapper.pausePayStubWorkflowStage0(request);
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetNotifications(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PausePayStubWorkflow", request.getPayStub().getPayStubId());
        return PausePayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a pay stub.
     */
    public ResumePayStubWorkflowResponse resumePayStubWorkflow(ResumePayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumePayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var futures0 = request.getPayStub().getLinesList().stream()
                .map(element -> RecalculatePayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePayStubs(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RecalculatePayStubsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumePayStubWorkflow", request.getPayStub().getPayStubId());
        return ResumePayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setNotes(request.getPayStub().getNotes())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public InspectPayStubWorkflowResponse inspectPayStubWorkflow(InspectPayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "InspectPayStubWorkflow");

        var outbound0 = ResumeRenewalWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .setParticipantId(request.getPayStubId())
                                .build())
                        .build())
                .build();
        var reply0 = contractsRenewalWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeRenewalWorkflow(outbound0);
        return InspectPayStubWorkflowResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRenewal().getAudit().getEtag())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a pay stub.
     */
    public SignalPayStubWorkflowResponse signalPayStubWorkflow(SignalPayStubWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPayStubWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var futures0 = request.getPayStub().getLinesList().stream()
                .map(element -> CommentMandateApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentMandateApproval(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CommentMandateApprovalResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CheckAuditTrailEligibilityRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setBic(switch (request.getPayStub().getSettlementCase()) {
                                    case BANK -> request.getPayStub().getBank().getBic();
                                    default -> "";
                                })
                                .setIban(switch (request.getPayStub().getSettlementCase()) {
                                    case BANK -> request.getPayStub().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .setVersion(request.getPayStub().getCounterparty().getVersion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAuditTrailEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPayStubWorkflow", request.getPayStub().getPayStubId());
        return SignalPayStubWorkflowResponse.newBuilder()
                .setOutcome(SignalPayStubWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setIssuedAt(request.getPayStub().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }
}
