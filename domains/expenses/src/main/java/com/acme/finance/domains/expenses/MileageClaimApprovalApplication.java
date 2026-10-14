package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.banking.v1.ListPendingStandingOrdersRequest;
import com.acme.banking.v1.ListPendingStandingOrdersResponse;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.ListPendingBudgetLinesRequest;
import com.acme.budgeting.v1.ListPendingBudgetLinesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.expenses.v1.ApproveMileageClaimRequest;
import com.acme.expenses.v1.ApproveMileageClaimResponse;
import com.acme.expenses.v1.CommentMileageClaimApprovalRequest;
import com.acme.expenses.v1.CommentMileageClaimApprovalResponse;
import com.acme.expenses.v1.EscalateMileageClaimRequest;
import com.acme.expenses.v1.EscalateMileageClaimResponse;
import com.acme.expenses.v1.ExpireMileageClaimRequest;
import com.acme.expenses.v1.ExpireMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusRequest;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusResponse;
import com.acme.expenses.v1.ListPendingReceiptsRequest;
import com.acme.expenses.v1.ListPendingReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.RejectMileageClaimRequest;
import com.acme.expenses.v1.RejectMileageClaimResponse;
import com.acme.expenses.v1.RenewMileageClaimRequest;
import com.acme.expenses.v1.RenewMileageClaimResponse;
import com.acme.expenses.v1.SubmitMileageClaimRequest;
import com.acme.expenses.v1.SubmitMileageClaimResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.licensing.v1.CreateSoftwareLicenseRequest;
import com.acme.licensing.v1.CreateSoftwareLicenseResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseCardFunding;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.webhooks.v1.ListWebhookEndpointsRequest;
import com.acme.webhooks.v1.ListWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes mileage claims through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class MileageClaimApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimApprovalApplication.class);

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceBlockingStub expensesMileageClaimLifecycleServiceBlocking;

    private final ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking;

    private final SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking;

    public MileageClaimApprovalApplication(
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceBlockingStub expensesMileageClaimLifecycleServiceBlocking,
            ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking,
            SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking) {
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.budgetingBudgetLineApprovalServiceBlocking = budgetingBudgetLineApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.expensesMileageClaimLifecycleServiceBlocking = expensesMileageClaimLifecycleServiceBlocking;
        this.expensesReceiptApprovalServiceBlocking = expensesReceiptApprovalServiceBlocking;
        this.licensingSoftwareLicenseServiceBlocking = licensingSoftwareLicenseServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.webhooksWebhookEndpointServiceBlocking = webhooksWebhookEndpointServiceBlocking;
    }

    /**
     * Submits a mileage claim.
     */
    public SubmitMileageClaimResponse submitMileageClaim(SubmitMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = QuoteConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getMileageClaim().getBillingAddress().getCountryCode())
                                .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.ConsentRecordCalculationService");
        var serviceName0 = "acme.customers.v1" + "." + "ConsentRecordCalculationService";
        var methodName0 = "Quo" + "teConsentRecord";
        var descriptor0 = MethodDescriptor.<QuoteConsentRecordRequest, QuoteConsentRecordResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteConsentRecordRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteConsentRecordResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ListPendingReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingReceiptsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<ListPendingReceiptsRequest, ListPendingReceiptsResponse> port1 = RpcPort.adapt(expensesReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listPendingReceipts);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ReconcileAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setLast4(request.getMileageClaim().getCard().getLast4())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAuditTrails(outbound2);

        var outbound3 = SignalAttestationWorkflowRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.compliance.v1.AttestationWorkflowService");
        var selected3 = AttestationWorkflowServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.signalAttestationWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitMileageClaim", request.getMileageClaim().getMileageClaimId());
        return SubmitMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(request.getMileageClaim().getBillingAddress())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setTotal(reply2.getAuditTrail().getTotal())
                        .build())
                .setOutcome(SubmitMileageClaimResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves a mileage claim.
     */
    public ApproveMileageClaimResponse approveMileageClaim(ApproveMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = CreateSoftwareLicenseRequest.newBuilder()
                .setContext(request.getContext())
                .setSoftwareLicense(SoftwareLicense.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .build())
                        .setCard(SoftwareLicenseCardFunding.newBuilder()
                                .setNetwork(request.getMileageClaim().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        Function<CreateSoftwareLicenseRequest, CreateSoftwareLicenseResponse> operation0 = licensingSoftwareLicenseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createSoftwareLicense;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = BeanCopies.copy(ExpireMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpireMileageClaimRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getSoftwareLicense().getHolder().getDisplayName())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setName(reply0.getSoftwareLicense().getContact().getName())
                                .setPreferredChannel(reply0.getSoftwareLicense().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setMileageClaimId(request.getMileageClaim().getMileageClaimId())
                .build(), ExpireMileageClaimRequest.class);
        var reply1 = expensesMileageClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expireMileageClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveMileageClaim", request.getMileageClaim().getNumber());
        return ApproveMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNotes(request.getMileageClaim().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a mileage claim.
     */
    public RejectMileageClaimResponse rejectMileageClaim(RejectMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = ListPendingStandingOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingStandingOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingStandingOrders(outbound0), rpcExecutor);

        var outbound1 = ListWebhookEndpointsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListWebhookEndpointsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMileageClaim().getAudit().getUpdatedAt())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> webhooksWebhookEndpointServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listWebhookEndpoints(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectMileageClaim", request.getMileageClaim().getMileageClaimId());
        return RejectMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .setTotal(request.getMileageClaim().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a mileage claim.
     */
    public EscalateMileageClaimResponse escalateMileageClaim(EscalateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = JsonCodec.roundTrip(RenewMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RenewMileageClaimRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(reply0.getCustomerSegment().getOwner().getUserId())
                                .build())
                        .build())
                .setMileageClaimId(request.getMileageClaim().getMileageClaimId())
                .build(), RenewMileageClaimRequest.class);
        var reply1 = expensesMileageClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).renewMileageClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateMileageClaim", request.getMileageClaim().getNumber());
        return EscalateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setDisplayName(reply0.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getMileageClaim().getDueAt())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public GetMileageClaimApprovalStatusResponse getMileageClaimApprovalStatus(GetMileageClaimApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMileageClaimApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "GetMileageClaimApprovalStatus");

        var outbound0 = RpcSupport.unpack(Any.pack(ExpireMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpireMileageClaimRequest.Details.newBuilder()
                        .setEffectiveAt(request.getContext().getDeadline())
                        .setNewOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setMileageClaimId(request.getMileageClaimId())
                .build()), ExpireMileageClaimRequest.class);
        var reply0 = expensesMileageClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expireMileageClaim(outbound0);
        return GetMileageClaimApprovalStatusResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getMileageClaim().getAudit().getCreatedBy())
                                .setEtag(reply0.getMileageClaim().getAudit().getEtag())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .build();
    }

    /**
     * Comments a mileage claim.
     */
    public CommentMileageClaimApprovalResponse commentMileageClaimApproval(CommentMileageClaimApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentMileageClaimApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = ListPendingBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingBudgetLinesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMileageClaim().getAudit().getCreatedAt())
                        .putAllLabels(request.getMileageClaim().getMetadataMap())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> budgetingBudgetLineApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingBudgetLines(outbound0), rpcExecutor);

        var outbound1 = PauseCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getMileageClaim().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCollectionCaseWorkflow(outbound1), rpcExecutor);

        var outbound2 = ListDepreciationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMileageClaim().getAudit().getCreatedAt())
                        .setOwnerId(request.getMileageClaim().getCounterparty().getReceiptId())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationReports(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentMileageClaimApproval", request.getMileageClaim().getMileageClaimId());
        return CommentMileageClaimApprovalResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setDisplayName(reply1.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setStatus(request.getMileageClaim().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
