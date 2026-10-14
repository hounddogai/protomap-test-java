package com.acme.finance.domains.treasury;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionRequest;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionResponse;
import com.acme.expenses.v1.EscalateReceiptRequest;
import com.acme.expenses.v1.EscalateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyApprovalServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ListPendingExpenseReportsRequest;
import com.acme.expenses.v1.ListPendingExpenseReportsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.SubmitExpensePolicyRequest;
import com.acme.expenses.v1.SubmitExpensePolicyResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountLimit;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.treasury.v1.ActivateLiquidityForecastRequest;
import com.acme.treasury.v1.ActivateLiquidityForecastResponse;
import com.acme.treasury.v1.ApproveLiquidityForecastRequest;
import com.acme.treasury.v1.ApproveLiquidityForecastResponse;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.CommentFxHedgeApprovalRequest;
import com.acme.treasury.v1.CommentFxHedgeApprovalResponse;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.DelegateLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.DelegateLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.EscalateLiquidityForecastRequest;
import com.acme.treasury.v1.EscalateLiquidityForecastResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.GetLiquidityForecastApprovalStatusRequest;
import com.acme.treasury.v1.GetLiquidityForecastApprovalStatusResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastAttachment;
import com.acme.treasury.v1.LiquidityForecastBankSettlement;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLifecycleServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListCashPositionWorkflowsRequest;
import com.acme.treasury.v1.ListCashPositionWorkflowsResponse;
import com.acme.treasury.v1.ListLiquidityForecastConflictsRequest;
import com.acme.treasury.v1.ListLiquidityForecastConflictsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RecallLiquidityForecastRequest;
import com.acme.treasury.v1.RecallLiquidityForecastResponse;
import com.acme.treasury.v1.RejectLiquidityForecastRequest;
import com.acme.treasury.v1.RejectLiquidityForecastResponse;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictRequest;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.SubmitLiquidityForecastRequest;
import com.acme.treasury.v1.SubmitLiquidityForecastResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes liquidity forecasts through approval, with escalation and delegation. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class LiquidityForecastApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastApprovalApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceBlockingStub expensesExpensePolicyApprovalServiceBlocking;

    private final ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking;

    private final ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LiquidityForecastApprovalRequestMapper mapper;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking;

    private final CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking;

    private final FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    private final LiquidityForecastLifecycleServiceGrpc.LiquidityForecastLifecycleServiceBlockingStub treasuryLiquidityForecastLifecycleServiceBlocking;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    public LiquidityForecastApprovalApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceBlockingStub expensesExpensePolicyApprovalServiceBlocking,
            ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking,
            ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LiquidityForecastApprovalRequestMapper mapper,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking,
            CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking,
            FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking,
            LiquidityForecastLifecycleServiceGrpc.LiquidityForecastLifecycleServiceBlockingStub treasuryLiquidityForecastLifecycleServiceBlocking,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.events = events;
        this.expensesExpensePolicyApprovalServiceBlocking = expensesExpensePolicyApprovalServiceBlocking;
        this.expensesExpenseReportApprovalServiceBlocking = expensesExpenseReportApprovalServiceBlocking;
        this.expensesReceiptApprovalServiceBlocking = expensesReceiptApprovalServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryCashPositionApprovalServiceBlocking = treasuryCashPositionApprovalServiceBlocking;
        this.treasuryCashPositionWorkflowServiceBlocking = treasuryCashPositionWorkflowServiceBlocking;
        this.treasuryFxHedgeApprovalServiceBlocking = treasuryFxHedgeApprovalServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
        this.treasuryLiquidityForecastLifecycleServiceBlocking = treasuryLiquidityForecastLifecycleServiceBlocking;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
    }

    public record SubmitLiquidityForecastStage0Requested(CreateUserRequest request, CompletableFuture<CreateUserResponse> completion, io.grpc.Context context) {}

    public record RecallLiquidityForecastStage0Requested(GetCashPositionLifecycleRequest request, CompletableFuture<GetCashPositionLifecycleResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a liquidity forecast.
     */
    public SubmitLiquidityForecastResponse submitLiquidityForecast(SubmitLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CreateUserResponse>();
        events.publishEvent(new SubmitLiquidityForecastStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = mapper.submitLiquidityForecastStage1(request);
        var reply1 = treasuryCashPositionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateCashPositionApproval(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return SubmitLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getUser().getRelationshipManager().getDisplayName())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .setNumber(reply1.getCashPosition().getHolder().getNumber())
                        .build())
                .setOutcome(SubmitLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Approves a liquidity forecast.
     */
    public ApproveLiquidityForecastResponse approveLiquidityForecast(ApproveLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ListCashPositionWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCashPositionWorkflowsRequest.Filter.newBuilder()
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> treasuryCashPositionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCashPositionWorkflows(value));
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return ApproveLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setTotal(request.getLiquidityForecast().getTotal())
                        .build())
                .setOutcome(ApproveLiquidityForecastResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a liquidity forecast.
     */
    public RejectLiquidityForecastResponse rejectLiquidityForecast(RejectLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = (SubmitExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitExpensePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .build())
                        .setBlock(ExpensePolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesExpensePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitExpensePolicy(outbound0);

        var outbound1 = SyncAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpensePolicy().getAudit().getEtag())
                                .setUpdatedBy(reply0.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SyncAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        RpcPort<SyncAssetTransfersRequest, SyncAssetTransfersResponse> port1 = RpcPort.adapt(assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::syncAssetTransfers);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ActivateLiquidityForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ActivateLiquidityForecastRequest.Details.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getAssetTransfer().getCounterparty().getDisplayName())
                                .setUserId(reply0.getExpensePolicy().getOwner().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setLiquidityForecastId(request.getLiquidityForecastId())
                .build();
        var adapter2 = new Function<ActivateLiquidityForecastRequest, ActivateLiquidityForecastResponse>() {
            @Override
            public ActivateLiquidityForecastResponse apply(ActivateLiquidityForecastRequest input) {
                return treasuryLiquidityForecastLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).activateLiquidityForecast(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return RejectLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getAssetTransfer().getBillingAddress().getLinesList())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNumber(reply2.getLiquidityForecast().getNumber())
                        .setStatus(request.getLiquidityForecast().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Escalates a liquidity forecast.
     */
    public EscalateLiquidityForecastResponse escalateLiquidityForecast(EscalateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ListLiquidityForecastConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLiquidityForecastConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLiquidityForecast().getAudit().getCreatedAt())
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLiquidityForecastConflicts(outbound0), rpcExecutor);

        var outbound1 = ExplainCustomerSegmentDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(ExplainCustomerSegmentDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentDecision(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return EscalateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNumber(request.getLiquidityForecast().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates a liquidity forecast.
     */
    public DelegateLiquidityForecastApprovalResponse delegateLiquidityForecastApproval(DelegateLiquidityForecastApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateLiquidityForecastApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = BeanCopies.copy(MapLiquidityForecastImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getLiquidityForecast().getBillingAddress().getCountryCode())
                                .setRecipient(request.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .setCard(LiquidityForecastCardSettlement.newBuilder()
                                .setLast4(request.getLiquidityForecast().getCard().getLast4())
                                .build())
                        .build())
                .build(), MapLiquidityForecastImportColumnsRequest.class);
        var reply0 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).mapLiquidityForecastImportColumns(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ResolveLiquidityForecastConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveLiquidityForecastConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBank(LiquidityForecastBankSettlement.newBuilder()
                                .setBic(request.getLiquidityForecast().getBank().getBic())
                                .build())
                        .build())
                .build()), ResolveLiquidityForecastConflictRequest.class);
        var reply1 = treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLiquidityForecastConflict(outbound1);

        var outbound2 = PushPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushPremiumRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(value));

        var outbound3 = JsonCodec.roundTrip(ListPendingExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingExpenseReportsRequest.Filter.newBuilder()
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build(), ListPendingExpenseReportsRequest.class);
        var reply3 = expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingExpenseReports(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateLiquidityForecastApproval", request.getLiquidityForecast().getLiquidityForecastId());
        return DelegateLiquidityForecastApprovalResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getPremium().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(reply1.getLiquidityForecast().getBillingAddress())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getLiquidityForecast().getCounterparty().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setTotal(request.getLiquidityForecast().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a liquidity forecast.
     */
    public RecallLiquidityForecastResponse recallLiquidityForecast(RecallLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = GetCashPositionLifecycleRequest.newBuilder()
                .setCashPositionId(request.getLiquidityForecast().getCounterparty().getCashPositionId())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<GetCashPositionLifecycleResponse>();
        events.publishEvent(new RecallLiquidityForecastStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var results1 = new ArrayList<RecordLedgerAccountVersionResponse>();
        for (var element : request.getLiquidityForecast().getLinesList()) {
            RecordLedgerAccountVersionRequest itemRequest = RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setAccountHolder(request.getLiquidityForecast().getBank().getAccountHolder())
                                .setBic(reply0.getCashPosition().getBank().getBic())
                                .build())
                        .addLimits(LedgerAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build();
            results1.add(ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(itemRequest));
        }
        var reply1 = results1.isEmpty() ? RecordLedgerAccountVersionResponse.getDefaultInstance()
                : results1.getLast();

        var results2 = new ArrayList<RetryLiquidityForecastImportResponse>();
        for (var element : request.getLiquidityForecast().getAttachmentsList()) {
            RetryLiquidityForecastImportRequest itemRequest = RetryLiquidityForecastImportRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .addAttachments(LiquidityForecastAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build();
            results2.add(treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryLiquidityForecastImport(itemRequest));
        }
        var reply2 = results2.isEmpty() ? RetryLiquidityForecastImportResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CreateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command3 = new DispatchCommand<>("acme.audit.v1.AuditEventService/CreateAuditEvent", outbound3);
        var reply3 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAuditEvent(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return RecallLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCashPosition().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setOutcome(RecallLiquidityForecastResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastApprovalStatusResponse getLiquidityForecastApprovalStatus(GetLiquidityForecastApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastApprovalStatus");
        return GetLiquidityForecastApprovalStatusResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
    }

    /**
     * Comments a liquidity forecast.
     */
    public CommentLiquidityForecastApprovalResponse commentLiquidityForecastApproval(CommentLiquidityForecastApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentLiquidityForecastApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = CommentFxHedgeApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBank(FxHedgeBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getLiquidityForecast().getSettlementCase()) {
                                    case BANK -> request.getLiquidityForecast().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getLiquidityForecast().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = treasuryFxHedgeApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentFxHedgeApproval(outbound0);

        var outbound1 = CommentPremiumApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getLiquidityForecast().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setHolderName(reply0.getFxHedge().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPremiumApproval(outbound1);

        var futures2 = request.getLiquidityForecast().getLinesList().stream()
                .map(element -> EscalateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateReceipt(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? EscalateReceiptResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentLiquidityForecastApproval", request.getLiquidityForecast().getLiquidityForecastId());
        return CommentLiquidityForecastApprovalResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFxHedge().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getFxHedge().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(request.getLiquidityForecast().getCounterparty())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(CommentLiquidityForecastApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
