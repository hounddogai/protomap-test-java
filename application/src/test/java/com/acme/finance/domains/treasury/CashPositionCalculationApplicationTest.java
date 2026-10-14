package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.SuggestReceiptsRequest;
import com.acme.expenses.v1.SuggestReceiptsResponse;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.performance.v1.SummarizeReviewsRequest;
import com.acme.performance.v1.SummarizeReviewsResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.SimulateCashPositionRequest;
import com.acme.treasury.v1.SimulateCashPositionResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CashPositionCalculationApplicationTest {
    @Test
    void explainCashPositionCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new CashPositionCalculationApplicationEvents(ContractClauseQueryServiceGrpc.newStub(channel), ExpenseReportCalculationServiceGrpc.newStub(channel), LoanValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CashPositionCalculationApplication.CalculateCashPositionStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof CashPositionCalculationApplication.QuoteCashPositionStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof CashPositionCalculationApplication.SimulateCashPositionStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CashPositionCalculationApplication(
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                events,
                ReceiptQueryServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                new CashPositionCalculationRequestMapperImpl(),
                ReviewQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newStub(channel),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainCashPositionCalculationRequest.getDefaultInstance());
        var response = application.explainCashPositionCalculation(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "contact", "phone"),
                ProbeSupport.value(sent0, "budget", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "audit", "etag"),
                ProbeSupport.value(sent0, "budget", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "budget", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/RevertLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets"), "budget", "bank", "iban"),
                ProbeSupport.value(sent2, "ledger_account", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "audit", "updated_at"),
                ProbeSupport.value(sent2, "ledger_account", "audit", "updated_at"));
        var sent3 = channel.request("acme.collections.v1.CollectionCaseQueryService/ExportCollectionCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RevertLedgerAccount"), "ledger_account", "labels"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "holder", "fx_hedge_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets"), "budget", "audit", "etag"),
                ProbeSupport.value(response, "cash_position", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RevertLedgerAccount"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(response, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "balance"),
                ProbeSupport.value(response, "cash_position", "balance"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CashPositionCalculationApplicationEvents(ContractClauseQueryServiceGrpc.newStub(channel), ExpenseReportCalculationServiceGrpc.newStub(channel), LoanValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculateExpenseReportResponse>();
        listener.on(new CashPositionCalculationApplication.CalculateCashPositionStage2Requested(ProbeSupport.sample(CalculateExpenseReportRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<PreviewLoanResponse>();
        listener.on(new CashPositionCalculationApplication.QuoteCashPositionStage1Requested(ProbeSupport.sample(PreviewLoanRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<FindContractClausesResponse>();
        listener.on(new CashPositionCalculationApplication.SimulateCashPositionStage0Requested(ProbeSupport.sample(FindContractClausesRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
