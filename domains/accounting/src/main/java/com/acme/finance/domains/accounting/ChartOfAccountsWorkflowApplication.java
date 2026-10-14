package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CheckReconciliationEligibilityRequest;
import com.acme.accounting.v1.CheckReconciliationEligibilityResponse;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsRequest;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.NormalizeReconciliationRequest;
import com.acme.accounting.v1.NormalizeReconciliationResponse;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.accounting.v1.UpdateChartOfAccountsRequest;
import com.acme.accounting.v1.UpdateChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a chart of accounts from start to finish. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class ChartOfAccountsWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ChartOfAccountsWorkflowApplication.class);

    private final AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking;

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final ChartOfAccountsWorkflowRequestMapper mapper;

    private final AccountingRepository repository;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    public ChartOfAccountsWorkflowApplication(
            AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking,
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            ChannelRegistry channels,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            ChartOfAccountsWorkflowRequestMapper mapper,
            AccountingRepository repository,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingAccrualApprovalServiceBlocking = accountingAccrualApprovalServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.accountingChartOfAccountsServiceBlocking = accountingChartOfAccountsServiceBlocking;
        this.accountingReconciliationValidationServiceBlocking = accountingReconciliationValidationServiceBlocking;
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.channels = channels;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a chart of accounts.
     */
    public StartChartOfAccountsWorkflowResponse startChartOfAccountsWorkflow(StartChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = PullUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingSyncService");
        var serviceName0 = "acme.insurance.v1" + "." + "UnderwritingSyncService";
        var methodName0 = "Pul" + "lUnderwritings";
        var descriptor0 = MethodDescriptor.<PullUnderwritingsRequest, PullUnderwritingsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(PullUnderwritingsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PullUnderwritingsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return StartChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a chart of accounts.
     */
    public AdvanceChartOfAccountsWorkflowResponse advanceChartOfAccountsWorkflow(AdvanceChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = RecallAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getChartOfAccounts().getLocation().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(accountingAccrualApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallAccrual(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return AdvanceChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply0.getAccrual().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
    }

    /**
     * Cancels a chart of accounts.
     */
    public CancelChartOfAccountsWorkflowResponse cancelChartOfAccountsWorkflow(CancelChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = JsonCodec.roundTrip(NormalizeAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), NormalizeAllocationRequest.class);
        var reply0 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAllocation(outbound0);

        var outbound1 = ScoreReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getAllocation().getOwner().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ScoreReconciliationResponse fetch(ScoreReconciliationRequest input) {
                return accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreReconciliation(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(NormalizeReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setPath(reply1.getReconciliation().getCustomer().getPath())
                                .build())
                        .build())
                .build()), NormalizeReconciliationRequest.class);
        var reply2 = accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeReconciliation(outbound2);

        var outbound3 = JsonCodec.roundTrip(RecallAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAllocation().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getChartOfAccounts().getLocation().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecallAccrualRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply2.getReconciliation().getAssignee().getEmail())
                                .build())
                        .build())
                .build(), RecallAccrualRequest.class);
        var reply3 = accountingAccrualApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallAccrual(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return CancelChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getAccrual().getAudit().getCreatedAt())
                                .setCreatedBy(reply2.getReconciliation().getAudit().getCreatedBy())
                                .setEtag(reply3.getAccrual().getAudit().getEtag())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(reply1.getReconciliation().getAudit().getEtag())
                                        .build())
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply2.getReconciliation().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getAllocation().getOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(request.getChartOfAccounts().getLocation())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a chart of accounts.
     */
    public RetryChartOfAccountsWorkflowResponse retryChartOfAccountsWorkflow(RetryChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = mapper.retryChartOfAccountsWorkflowStage0(request);
        var reply0 = accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitChartOfAccounts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return RetryChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getChartOfAccounts().getLocation().getPostalCode())
                                .build())
                        .setName(request.getChartOfAccounts().getName())
                        .build())
                .build();
    }

    /**
     * Pauses a chart of accounts.
     */
    public PauseChartOfAccountsWorkflowResponse pauseChartOfAccountsWorkflow(PauseChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = (CheckReconciliationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckReconciliationEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReconciliationEligibility(outbound0);

        var outbound1 = CancelPaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.PaymentPromiseWorkflowService");
        var selected1 = PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.cancelPaymentPromiseWorkflow(outbound1);

        var outbound2 = ValidateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setPath(switch (reply0.getReconciliation().getOriginCase()) {
                                    case CUSTOMER -> reply0.getReconciliation().getCustomer().getPath();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getReconciliation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateReconciliation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return PauseChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(reply1.getPaymentPromise().getAudit().getUpdatedBy())
                                                .build())
                                        .setChartOfAccountsId(reply2.getReconciliation().getCustomer().getChartOfAccountsId())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(reply0.getReconciliation().getRequester().getEmail())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setUserId(reply0.getReconciliation().getAssignee().getUserId())
                                .build())
                        .setStatus(request.getChartOfAccounts().getStatus())
                        .build())
                .setOutcome(PauseChartOfAccountsWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes a chart of accounts.
     */
    public ResumeChartOfAccountsWorkflowResponse resumeChartOfAccountsWorkflow(ResumeChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = ValidateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateReconciliation(outbound0);

        var outbound1 = (UpdateChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getReconciliation().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = accountingChartOfAccountsServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateChartOfAccounts(outbound1);

        var outbound2 = mapper.resumeChartOfAccountsWorkflowStage2(request);
        var reply2 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generatePremiumReport(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeChartOfAccountsWorkflow", request.getChartOfAccountsId());
        return ResumeChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .putAllAttributes(reply1.getChartOfAccounts().getAttributesMap())
                        .setChartOfAccountsId(reply0.getReconciliation().getCustomer().getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply0.getReconciliation().getAudit().getEtag())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply1.getChartOfAccounts().getLocation().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(request.getChartOfAccounts().getLocation())
                        .setStatus(request.getChartOfAccounts().getStatus())
                        .build())
                .setOutcome(ResumeChartOfAccountsWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a chart of accounts.
     */
    public InspectChartOfAccountsWorkflowResponse inspectChartOfAccountsWorkflow(InspectChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "InspectChartOfAccountsWorkflow");

        var outbound0 = SuggestJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestJournalEntriesRequest.Filter.newBuilder()
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getChartOfAccountsId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestJournalEntries(outbound0));
        return InspectChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    public ListChartOfAccountsWorkflowsResponse listChartOfAccountsWorkflows(ListChartOfAccountsWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListChartOfAccountsWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "ListChartOfAccountsWorkflows");

        var outbound0 = RpcSupport.unpack(Any.pack(ResolveAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveAuditTrailsRequest.class);
        var reply0 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrails(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListChartOfAccountsWorkflows", request.getPage().getPageToken());
        return ListChartOfAccountsWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Signals a chart of accounts.
     */
    public SignalChartOfAccountsWorkflowResponse signalChartOfAccountsWorkflow(SignalChartOfAccountsWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalChartOfAccountsWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = ValidateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getChartOfAccounts().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAllocationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAllocation(outbound0), rpcExecutor);

        var outbound1 = RetryRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryRiskScenarioWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalChartOfAccountsWorkflow", request.getChartOfAccounts().getChartOfAccountsId());
        return SignalChartOfAccountsWorkflowResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedBy(reply1.getRiskScenario().getAudit().getCreatedBy())
                                        .build())
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply1.getRiskScenario().getAudit().getEtag())
                                                .build())
                                        .build())
                                .build())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .build())
                .setOutcome(SignalChartOfAccountsWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
