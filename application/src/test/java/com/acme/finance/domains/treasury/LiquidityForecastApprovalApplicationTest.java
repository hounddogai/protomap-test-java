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
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
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
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class LiquidityForecastApprovalApplicationTest {
    @Test
    void recallLiquidityForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new LiquidityForecastApprovalApplicationEvents(UserServiceGrpc.newStub(channel), CashPositionLifecycleServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LiquidityForecastApprovalApplication.SubmitLiquidityForecastStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof LiquidityForecastApprovalApplication.RecallLiquidityForecastStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LiquidityForecastApprovalApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyApprovalServiceGrpc.newBlockingStub(channel),
                ExpenseReportApprovalServiceGrpc.newBlockingStub(channel),
                ReceiptApprovalServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                new LiquidityForecastApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                CashPositionApprovalServiceGrpc.newBlockingStub(channel),
                CashPositionWorkflowServiceGrpc.newBlockingStub(channel),
                FxHedgeApprovalServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel),
                LiquidityForecastLifecycleServiceGrpc.newBlockingStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallLiquidityForecastRequest.getDefaultInstance());
        var response = application.recallLiquidityForecast(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "counterparty", "cash_position_id"),
                ProbeSupport.value(sent0, "cash_position_id"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "lines", "amount"),
                ProbeSupport.value(sent1, "ledger_account", "limits", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "bank", "bic"),
                ProbeSupport.value(sent1, "ledger_account", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "bank", "account_holder"),
                ProbeSupport.value(sent1, "ledger_account", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "audit", "created_at"),
                ProbeSupport.value(sent1, "ledger_account", "audit", "created_at"));
        var sent2 = channel.request("acme.treasury.v1.LiquidityForecastImportService/RetryLiquidityForecastImport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "attachments", "checksum"),
                ProbeSupport.value(sent2, "liquidity_forecast", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "attachments", "file_name"),
                ProbeSupport.value(sent2, "liquidity_forecast", "attachments", "file_name"));
        var sent3 = channel.request("acme.audit.v1.AuditEventService/CreateAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "holder", "display_name"),
                ProbeSupport.value(sent3, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastImportService/RetryLiquidityForecastImport"), "result", "correlation_id"),
                ProbeSupport.value(sent3, "audit_event", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "audit", "updated_at"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "audit", "created_at"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/CreateAuditEvent"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "metadata"),
                ProbeSupport.value(response, "liquidity_forecast", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new LiquidityForecastApprovalApplicationEvents(UserServiceGrpc.newStub(channel), CashPositionLifecycleServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CreateUserResponse>();
        listener.on(new LiquidityForecastApprovalApplication.SubmitLiquidityForecastStage0Requested(ProbeSupport.sample(CreateUserRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<GetCashPositionLifecycleResponse>();
        listener.on(new LiquidityForecastApprovalApplication.RecallLiquidityForecastStage0Requested(ProbeSupport.sample(GetCashPositionLifecycleRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
