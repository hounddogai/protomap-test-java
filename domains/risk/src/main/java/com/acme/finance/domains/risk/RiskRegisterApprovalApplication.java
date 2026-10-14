package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.configuration.v1.ConfigSet;
import com.acme.configuration.v1.ConfigSetCalculationServiceGrpc;
import com.acme.configuration.v1.EstimateConfigSetRequest;
import com.acme.configuration.v1.EstimateConfigSetResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.pipelines.v1.PipelineServiceGrpc;
import com.acme.pipelines.v1.SearchPipelinesRequest;
import com.acme.pipelines.v1.SearchPipelinesResponse;
import com.acme.risk.v1.ApproveRiskRegisterRequest;
import com.acme.risk.v1.ApproveRiskRegisterResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CommentRiskRegisterApprovalRequest;
import com.acme.risk.v1.CommentRiskRegisterApprovalResponse;
import com.acme.risk.v1.DelegateRiskRegisterApprovalRequest;
import com.acme.risk.v1.DelegateRiskRegisterApprovalResponse;
import com.acme.risk.v1.EscalateRiskRegisterRequest;
import com.acme.risk.v1.EscalateRiskRegisterResponse;
import com.acme.risk.v1.FindRiskRegistersRequest;
import com.acme.risk.v1.FindRiskRegistersResponse;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusRequest;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusResponse;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes risk registers through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskRegisterApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskRegisterApprovalApplication.class);

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConfigSetCalculationServiceGrpc.ConfigSetCalculationServiceBlockingStub configurationConfigSetCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final PipelineServiceGrpc.PipelineServiceBlockingStub pipelinesPipelineServiceBlocking;

    private final RiskRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    private final StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public RiskRegisterApprovalApplication(
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            ChannelRegistry channels,
            ConfigSetCalculationServiceGrpc.ConfigSetCalculationServiceBlockingStub configurationConfigSetCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            PipelineServiceGrpc.PipelineServiceBlockingStub pipelinesPipelineServiceBlocking,
            RiskRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking,
            StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.channels = channels;
        this.configurationConfigSetCalculationServiceBlocking = configurationConfigSetCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.pipelinesPipelineServiceBlocking = pipelinesPipelineServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
        this.riskStressTestLifecycleServiceBlocking = riskStressTestLifecycleServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    /**
     * Submits a risk register.
     */
    public SubmitRiskRegisterResponse submitRiskRegister(SubmitRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = BrowseRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getRiskRegister().getRiskRegisterId())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskRegisterQueryService");
        var selected0 = RiskRegisterQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.browseRiskRegisters(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitRiskRegister", request.getRiskRegister().getName());
        return SubmitRiskRegisterResponse.newBuilder()
                .setOutcome(SubmitRiskRegisterResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .putAllAttributes(request.getRiskRegister().getAttributesMap())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Approves a risk register.
     */
    public ApproveRiskRegisterResponse approveRiskRegister(ApproveRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = EstimateConfigSetRequest.newBuilder()
                .setConfigSet(ConfigSet.newBuilder()
                        .addChildren(ConfigSet.newBuilder()
                                .addChildren(ConfigSet.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getRiskRegister().getLocation().getLocality())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> configurationConfigSetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateConfigSet(outbound0), rpcExecutor);

        var outbound1 = LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRiskRegister().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return ApproveRiskRegisterResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addAllPath(reply0.getConfigSet().getPathList())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(reply1.getCustomer().getContact().getEmail())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .setStatus(request.getRiskRegister().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a risk register.
     */
    public RejectRiskRegisterResponse rejectRiskRegister(RejectRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = PreviewCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomerSegment(outbound0), rpcExecutor);

        var outbound1 = NormalizeSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRisk(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return RejectRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .putAllAttributes(request.getRiskRegister().getAttributesMap())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply1.getSupplierRisk().getAssignee().getDisplayName())
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Escalates a risk register.
     */
    public EscalateRiskRegisterResponse escalateRiskRegister(EscalateRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = BeanCopies.copy(FindRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getRiskRegister().getAttributesMap())
                        .setOwnerId(request.getRiskRegister().getParentId())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build(), FindRiskRegistersRequest.class);
        var reply0 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findRiskRegisters(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return EscalateRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setName(request.getRiskRegister().getName())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public ListPendingRiskRegistersResponse listPendingRiskRegisters(ListPendingRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "ListPendingRiskRegisters");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingRiskRegisters", request.getPage().getPageToken());
        return ListPendingRiskRegistersResponse.getDefaultInstance();
    }

    /**
     * Delegates a risk register.
     */
    public DelegateRiskRegisterApprovalResponse delegateRiskRegisterApproval(DelegateRiskRegisterApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateRiskRegisterApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = LintStressTestsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintStressTestsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRiskRegister().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRiskRegister().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.StressTestValidationService");
        var selected0 = StressTestValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.lintStressTests(outbound0);

        var outbound1 = SendAuditTrailTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendAuditTrailTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendAuditTrailTestNotice(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateRiskRegisterApproval", request.getRiskRegister().getRiskRegisterId());
        return DelegateRiskRegisterApprovalResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getStressTest().getAudit().getUpdatedBy())
                                .build())
                        .addChildren(RiskRegister.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                                        .build())
                                .build())
                        .setManager(request.getRiskRegister().getManager())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Recalls a risk register.
     */
    public RecallRiskRegisterResponse recallRiskRegister(RecallRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getUpdatedAt())
                        .setOwnerId(request.getRiskRegister().getManager().getUserId())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound0);
        var reply0 = WatchDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SearchPipelinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPipelinesRequest.Filter.newBuilder()
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.pipelines.v1.PipelineService/SearchPipelines", outbound1);
        var reply1 = pipelinesPipelineServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPipelines(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return RecallRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setParentId(request.getRiskRegister().getParentId())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Returns a risk register.
     */
    public GetRiskRegisterApprovalStatusResponse getRiskRegisterApprovalStatus(GetRiskRegisterApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRiskRegisterApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "GetRiskRegisterApprovalStatus");
        return GetRiskRegisterApprovalStatusResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Comments a risk register.
     */
    public CommentRiskRegisterApprovalResponse commentRiskRegisterApproval(CommentRiskRegisterApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentRiskRegisterApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = ScheduleStressTestExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleStressTestExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskStressTestLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleStressTestExpiry(value)).orElseThrow();

        var outbound1 = (ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getStressTest().getAudit().getCreatedAt())
                                .setEtag(request.getRiskRegister().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCustomerSegment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentRiskRegisterApproval", request.getRiskRegister().getRiskRegisterId());
        return CommentRiskRegisterApprovalResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(request.getRiskRegister().getLocation())
                        .setManager(UserRef.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .setName(reply1.getCustomerSegment().getName())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }
}
