package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ListCustomersRequest;
import com.acme.customers.v1.ListCustomersResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationAttachment;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowRequest;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowResponse;
import com.acme.risk.v1.ActivateStressTestRequest;
import com.acme.risk.v1.ActivateStressTestResponse;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.ApproveStressTestRequest;
import com.acme.risk.v1.ApproveStressTestResponse;
import com.acme.risk.v1.CommentStressTestApprovalRequest;
import com.acme.risk.v1.CommentStressTestApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.EscalateStressTestRequest;
import com.acme.risk.v1.EscalateStressTestResponse;
import com.acme.risk.v1.ExplainExposureDecisionRequest;
import com.acme.risk.v1.ExplainExposureDecisionResponse;
import com.acme.risk.v1.ExportRiskRegistersRequest;
import com.acme.risk.v1.ExportRiskRegistersResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetStressTestApprovalStatusRequest;
import com.acme.risk.v1.GetStressTestApprovalStatusResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PauseRiskRegisterWorkflowRequest;
import com.acme.risk.v1.PauseRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RecallStressTestRequest;
import com.acme.risk.v1.RecallStressTestResponse;
import com.acme.risk.v1.RejectStressTestRequest;
import com.acme.risk.v1.RejectStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.SignalRiskRegisterWorkflowRequest;
import com.acme.risk.v1.SignalRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.SubmitStressTestRequest;
import com.acme.risk.v1.SubmitStressTestResponse;
import com.acme.shipping.v1.ExportShipmentsRequest;
import com.acme.shipping.v1.ExportShipmentsResponse;
import com.acme.shipping.v1.ShipmentQueryServiceGrpc;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes stress tests through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StressTestApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(StressTestApprovalApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final RiskRepository repository;

    private final ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceFutureStub riskRiskScenarioApprovalServiceFuture;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking;

    private final Executor rpcExecutor;

    private final ShipmentQueryServiceGrpc.ShipmentQueryServiceBlockingStub shippingShipmentQueryServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    public StressTestApprovalApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            ChannelRegistry channels,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            RiskRepository repository,
            ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceFutureStub riskRiskScenarioApprovalServiceFuture,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShipmentQueryServiceGrpc.ShipmentQueryServiceBlockingStub shippingShipmentQueryServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.channels = channels;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.repository = repository;
        this.riskExposureApprovalServiceBlocking = riskExposureApprovalServiceBlocking;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.riskRiskScenarioApprovalServiceFuture = riskRiskScenarioApprovalServiceFuture;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.riskStressTestLifecycleServiceBlocking = riskStressTestLifecycleServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.shippingShipmentQueryServiceBlocking = shippingShipmentQueryServiceBlocking;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
    }

    /**
     * Submits a stress test.
     */
    public SubmitStressTestResponse submitStressTest(SubmitStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var futures0 = request.getStressTest().getLinesList().stream()
                .map(element -> PauseCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCoverageWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? PauseCoverageWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExportRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCoverage().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getCoverage().getOwner().getUserId())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportRiskRegisters(outbound1);
        var reply1 = ExportRiskRegistersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStressTest().getAudit().getUpdatedAt())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .setSince(reply0.getCoverage().getAudit().getUpdatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getStressTest().getCounterparty().getEmail())
                        .build())
                .build();
        var stream2 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound2);
        var reply2 = WatchDocumentsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = SignalRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getStressTest().getBillingAddress().getCountryCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.risk.v1.RiskRegisterWorkflowService");
        var selected3 = RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.signalRiskRegisterWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitStressTest", request.getStressTest().getStressTestId());
        return SubmitStressTestResponse.newBuilder()
                .setOutcome(SubmitStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getRiskRegister().getAudit().getUpdatedAt())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Approves a stress test.
     */
    public ApproveStressTestResponse approveStressTest(ApproveStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAuditEventRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getStressTest().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getStressTest().getBillingAddress().getPostalCode())
                                .setRegion(request.getStressTest().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent", outbound0);
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveStressTest", request.getStressTest().getStressTestId());
        return ApproveStressTestResponse.newBuilder()
                .setOutcome(ApproveStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Rejects a stress test.
     */
    public RejectStressTestResponse rejectStressTest(RejectStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = ExplainExposureDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setCard(ExposureCardFunding.newBuilder()
                                .setToken(switch (request.getStressTest().getSettlementCase()) {
                                    case CARD -> request.getStressTest().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExposureDecision(outbound0);

        var outbound1 = BeanCopies.copy(BrowseAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccessPoliciesRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getExposure().getLabelsMap())
                        .build())
                .build(), BrowseAccessPoliciesRequest.class);
        var reply1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(outbound1);

        var outbound2 = LintExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintExposuresRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStressTest().getAudit().getCreatedAt())
                                .build())
                        .setCard(ExposureCardFunding.newBuilder()
                                .setLast4(switch (reply0.getExposure().getFundingCase()) {
                                    case CARD -> reply0.getExposure().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExposures(outbound2);

        var outbound3 = NormalizeRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRiskScenarioRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply2.getExposure().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRiskScenario(value));
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectStressTest", request.getStressTest().getStressTestId());
        return RejectStressTestResponse.newBuilder()
                .setOutcome(RejectStressTestResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getRiskScenario().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getExposure().getHolder().getEmail())
                                .build())
                        .setStatus(request.getStressTest().getStatus())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Escalates a stress test.
     */
    public EscalateStressTestResponse escalateStressTest(EscalateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var results0 = new ArrayList<ExportShipmentsResponse>();
        for (var element : request.getStressTest().getLinesList()) {
            ExportShipmentsRequest itemRequest = ExportShipmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportShipmentsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
            var responses = shippingShipmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportShipments(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? ExportShipmentsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExplainExposureDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ExplainExposureDecisionRequest, ExplainExposureDecisionResponse>() {
            @Override
            public ExplainExposureDecisionResponse apply(ExplainExposureDecisionRequest input) {
                return riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExposureDecision(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateStressTest", request.getStressTest().getStressTestId());
        return EscalateStressTestResponse.newBuilder()
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getExposure().getAudit().getEtag())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .setTotal(request.getStressTest().getTotal())
                        .build())
                .build();
    }

    /**
     * Delegates a stress test.
     */
    public DelegateStressTestApprovalResponse delegateStressTestApproval(DelegateStressTestApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateStressTestApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = (EscalateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateStandingOrderRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateStandingOrder(outbound0);

        var outbound1 = ApproveRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveRiskScenarioRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = riskRiskScenarioApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).approveRiskScenario(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BeanCopies.copy(PauseRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getStressTest().getBillingAddress().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), PauseRiskRegisterWorkflowRequest.class);
        var reply2 = riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseRiskRegisterWorkflow(outbound2);

        var outbound3 = ActivateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ActivateStressTestRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getStandingOrder().getOrganizer().getEmail())
                                .setPhone(reply1.getRiskScenario().getRequester().getPhone())
                                .build())
                        .build())
                .setStressTestId(request.getStressTest().getStressTestId())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> riskStressTestLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).activateStressTest(value));
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateStressTestApproval", request.getStressTest().getNumber());
        return DelegateStressTestApprovalResponse.newBuilder()
                .setOutcome(DelegateStressTestApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply3.getStressTest().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply1.getRiskScenario().getCustomer().getCustomerId())
                                .build())
                        .addLines(StressTestLine.newBuilder()
                                .putAllAttributes(reply2.getRiskRegister().getAttributesMap())
                                .build())
                        .putAllMetadata(reply0.getStandingOrder().getMetadataMap())
                        .setStatus(request.getStressTest().getStatus())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Recalls a stress test.
     */
    public RecallStressTestResponse recallStressTest(RecallStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = DelegateExposureApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getStressTest().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskExposureApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateExposureApproval(outbound0), rpcExecutor);

        var outbound1 = NormalizeExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setBank(ExposureBankFunding.newBuilder()
                                .setBic(request.getStressTest().getBank().getBic())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeExposure(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = JsonCodec.roundTrip(RecallLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getStressTest().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .build(), RecallLoanApplicationRequest.class);
        var reply2 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallLoanApplication(outbound2);

        var outbound3 = ScoreExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExposure().getAudit().getCreatedAt())
                                .build())
                        .setCard(ExposureCardFunding.newBuilder()
                                .setLast4(switch (reply0.getExposure().getFundingCase()) {
                                    case CARD -> reply0.getExposure().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getExposure().getContact().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply3 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExposure(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallStressTest", request.getStressTest().getStressTestId());
        return RecallStressTestResponse.newBuilder()
                .setOutcome(RecallStressTestResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getLoanApplication().getBillingAddress().getLinesList())
                                .build())
                        .setStatus(request.getStressTest().getStatus())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Returns a stress test.
     */
    public GetStressTestApprovalStatusResponse getStressTestApprovalStatus(GetStressTestApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetStressTestApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "GetStressTestApprovalStatus");
        return GetStressTestApprovalStatusResponse.newBuilder()
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Comments a stress test.
     */
    public CommentStressTestApprovalResponse commentStressTestApproval(CommentStressTestApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentStressTestApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = FindSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.suppliers.v1.SupplierContractQueryService/FindSupplierContracts", outbound0);
        var reply0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSupplierContracts(command0.payload());

        var futures1 = request.getStressTest().getAttachmentsList().stream()
                .map(element -> ResumeLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addAttachments(LoanApplicationAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeLoanApplicationWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ResumeLoanApplicationWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAuditEventRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.AuditEventValidationService");
        var serviceName2 = "acme.audit.v1" + "." + "AuditEventValidationService";
        var methodName2 = "Sco" + "reAuditEvent";
        var descriptor2 = MethodDescriptor.<ScoreAuditEventRequest, ScoreAuditEventResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreAuditEventRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreAuditEventResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ListCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCustomersRequest.Filter.newBuilder()
                        .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<ListCustomersRequest, ListCustomersResponse> port3 = RpcPort.adapt(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listCustomers);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentStressTestApproval", request.getStressTest().getStressTestId());
        return CommentStressTestApprovalResponse.newBuilder()
                .setOutcome(CommentStressTestApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setDueAt(request.getStressTest().getDueAt())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }
}
