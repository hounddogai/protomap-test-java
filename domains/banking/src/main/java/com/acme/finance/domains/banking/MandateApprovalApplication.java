package com.acme.finance.domains.banking;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.banking.v1.AdvanceBankAccountWorkflowRequest;
import com.acme.banking.v1.AdvanceBankAccountWorkflowResponse;
import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountBankFunding;
import com.acme.banking.v1.BankAccountLimit;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelStatementWorkflowRequest;
import com.acme.banking.v1.CancelStatementWorkflowResponse;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.CommentMandateApprovalRequest;
import com.acme.banking.v1.CommentMandateApprovalResponse;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.DelegateMandateApprovalRequest;
import com.acme.banking.v1.DelegateMandateApprovalResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.GetMandateApprovalStatusRequest;
import com.acme.banking.v1.GetMandateApprovalStatusResponse;
import com.acme.banking.v1.ListPendingMandatesRequest;
import com.acme.banking.v1.ListPendingMandatesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.banking.v1.RejectMandateRequest;
import com.acme.banking.v1.RejectMandateResponse;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementLine;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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
 * Routes mandates through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class MandateApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(MandateApprovalApplication.class);

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking;

    private final StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking;

    private final StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final MandateApprovalRequestMapper mapper;

    private final BankingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    public MandateApprovalApplication(
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking,
            StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking,
            StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            MandateApprovalRequestMapper mapper,
            BankingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking) {
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.bankingStatementApprovalServiceBlocking = bankingStatementApprovalServiceBlocking;
        this.bankingStatementQueryServiceBlocking = bankingStatementQueryServiceBlocking;
        this.bankingStatementWorkflowServiceBlocking = bankingStatementWorkflowServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
    }

    /**
     * Submits a mandate.
     */
    public SubmitMandateResponse submitMandate(SubmitMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound0), rpcExecutor);

        var outbound1 = CommentStatementApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMandate().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentStatementApproval(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input2 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMandate().getAudit().getCreatedAt())
                                .setCreatedBy(request.getMandate().getAudit().getCreatedBy())
                                .setEtag(request.getMandate().getAudit().getEtag())
                                .setUpdatedAt(request.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitMandate", request.getMandate().getMandateId());
        return SubmitMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCustomerSegment().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getStatement().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(request.getMandate().getCounterparty())
                        .setMandateId(request.getMandateId())
                        .build())
                .build();
    }

    /**
     * Approves a mandate.
     */
    public ApproveMandateResponse approveMandate(ApproveMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = SuggestBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMandate().getAudit().getUpdatedAt())
                        .setRegion(request.getMandate().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.BankAccountQueryService");
        var serviceName0 = "acme.banking.v1" + "." + "BankAccountQueryService";
        var methodName0 = "Sug" + "gestBankAccounts";
        var descriptor0 = MethodDescriptor.<SuggestBankAccountsRequest, SuggestBankAccountsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SuggestBankAccountsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SuggestBankAccountsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(outbound1);

        var outbound2 = ExportStatementsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportStatementsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMandate().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setRegion(reply1.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportStatements(outbound2);
        var reply2 = ExportStatementsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveMandate", request.getMandate().getMandateId());
        return ApproveMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .putAllMetadata(request.getMandate().getMetadataMap())
                        .build())
                .setOutcome(ApproveMandateResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a mandate.
     */
    public RejectMandateResponse rejectMandate(RejectMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = JsonCodec.roundTrip(CheckStandingOrderEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMandate().getAudit().getEtag())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getMandate().getBillingAddress().getLinesList())
                                .setRecipient(request.getMandate().getBillingAddress().getRecipient())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build(), CheckStandingOrderEligibilityRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkStandingOrderEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectMandate", request.getMandate().getMandateId());
        return RejectMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(request.getMandate().getCounterparty())
                        .setMandateId(request.getMandateId())
                        .build())
                .setOutcome(RejectMandateResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Escalates a mandate.
     */
    public EscalateMandateResponse escalateMandate(EscalateMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ResolveSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSuppliers(outbound0);

        var outbound1 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyNoticesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setRegion(request.getMandate().getBillingAddress().getRegion())
                        .build())
                .setSince(request.getMandate().getAudit().getCreatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound1);
        var reply1 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateMandate", request.getMandate().getMandateId());
        return EscalateMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .setStatus(request.getMandate().getStatus())
                        .build())
                .setOutcome(EscalateMandateResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists mandates a page at a time.
     */
    public ListPendingMandatesResponse listPendingMandates(ListPendingMandatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingMandates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Mandate", request.getContext().getTraceId(), "ListPendingMandates");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingMandates", request.getPage().getPageToken());
        return ListPendingMandatesResponse.getDefaultInstance();
    }

    /**
     * Delegates a mandate.
     */
    public DelegateMandateApprovalResponse delegateMandateApproval(DelegateMandateApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateMandateApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = CommentStatementApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setCard(StatementCardSettlement.newBuilder()
                                .setHolderName(switch (request.getMandate().getSettlementCase()) {
                                    case CARD -> request.getMandate().getCard().getHolderName();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getMandate().getSettlementCase()) {
                                    case CARD -> request.getMandate().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentStatementApproval(outbound0);

        List<AdvanceBankAccountWorkflowResponse> results1 = request.getMandate().getLinesList().stream()
                .map(element -> AdvanceBankAccountWorkflowRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getStatement().getAudit().getCreatedAt())
                                .build())
                        .setBank(BankAccountBankFunding.newBuilder()
                                .setBic(reply0.getStatement().getBank().getBic())
                                .setIban(reply0.getStatement().getBank().getIban())
                                .build())
                        .addLimits(BankAccountLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceBankAccountWorkflow(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? AdvanceBankAccountWorkflowResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateMandateApproval", request.getMandate().getMandateId());
        return DelegateMandateApprovalResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .setTotal(request.getMandate().getTotal())
                        .build())
                .setOutcome(DelegateMandateApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalls a mandate.
     */
    public RecallMandateResponse recallMandate(RecallMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ResolveBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ResolveBankAccountsRequest, ResolveBankAccountsResponse>() {
            @Override
            public ResolveBankAccountsResponse apply(ResolveBankAccountsRequest input) {
                return bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBankAccounts(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallMandate", request.getMandate().getMandateId());
        return RecallMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .setNumber(request.getMandate().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a mandate.
     */
    public GetMandateApprovalStatusResponse getMandateApprovalStatus(GetMandateApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMandateApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Mandate", request.getMandateId(), "GetMandateApprovalStatus");

        var outbound0 = CancelStatementWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelStatementWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(StatementLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.banking.v1.StatementWorkflowService/CancelStatementWorkflow", outbound0);
        var reply0 = bankingStatementWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelStatementWorkflow(command0.payload());

        var outbound1 = mapper.getMandateApprovalStatusStage1(request, reply0);
        var reply1 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPreferenceEligibility(outbound1);
        return GetMandateApprovalStatusResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPreference().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStatement().getBillingAddress().getCountryCode())
                                .build())
                        .setMandateId(request.getMandateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Comments a mandate.
     */
    public CommentMandateApprovalResponse commentMandateApproval(CommentMandateApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentMandateApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setDetails(RecalculateCustomerSegmentsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getMandate().getBillingAddress().getPostalCode())
                                .setRegion(request.getMandate().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(value)))
                .collect(Collectors.toList()).getFirst();

        var builder1 = EscalateStandingOrderRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setStandingOrder(StandingOrder.newBuilder()
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateStandingOrder(outbound1);

        var outbound2 = ApproveCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply1.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.CoverageApprovalService");
        var serviceName2 = "acme.insurance.v1" + "." + "CoverageApprovalService";
        var methodName2 = "App" + "roveCoverage";
        var descriptor2 = MethodDescriptor.<ApproveCoverageRequest, ApproveCoverageResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ApproveCoverageRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ApproveCoverageResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentMandateApproval", request.getMandate().getMandateId());
        return CommentMandateApprovalResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCoverage().getAudit().getEtag())
                                .build())
                        .setMandateId(request.getMandateId())
                        .build())
                .setOutcome(CommentMandateApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
