package com.acme.finance.domains.treasury;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.helpdesk.v1.FindTicketsRequest;
import com.acme.helpdesk.v1.FindTicketsResponse;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchAttachment;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.loyalty.v1.BrowseLoyaltyMembersRequest;
import com.acme.loyalty.v1.BrowseLoyaltyMembersResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowRequest;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CancelBankTransferWorkflowRequest;
import com.acme.treasury.v1.CancelBankTransferWorkflowResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CountBankTransferTotalsRequest;
import com.acme.treasury.v1.CountBankTransferTotalsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.InspectBankTransferWorkflowRequest;
import com.acme.treasury.v1.InspectBankTransferWorkflowResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLine;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListBankTransferWorkflowsRequest;
import com.acme.treasury.v1.ListBankTransferWorkflowsResponse;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.PauseBankTransferWorkflowRequest;
import com.acme.treasury.v1.PauseBankTransferWorkflowResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.ResumeBankTransferWorkflowRequest;
import com.acme.treasury.v1.ResumeBankTransferWorkflowResponse;
import com.acme.treasury.v1.RetryBankTransferWorkflowRequest;
import com.acme.treasury.v1.RetryBankTransferWorkflowResponse;
import com.acme.treasury.v1.ScoreFxHedgeRequest;
import com.acme.treasury.v1.ScoreFxHedgeResponse;
import com.acme.treasury.v1.SignalBankTransferWorkflowRequest;
import com.acme.treasury.v1.SignalBankTransferWorkflowResponse;
import com.acme.treasury.v1.StartBankTransferWorkflowRequest;
import com.acme.treasury.v1.StartBankTransferWorkflowResponse;
import com.acme.treasury.v1.StreamBankTransferMetricsRequest;
import com.acme.treasury.v1.StreamBankTransferMetricsResponse;
import com.acme.treasury.v1.SubmitFxHedgeRequest;
import com.acme.treasury.v1.SubmitFxHedgeResponse;
import com.acme.treasury.v1.SuggestBankTransfersRequest;
import com.acme.treasury.v1.SuggestBankTransfersResponse;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BankTransferWorkflowApplicationTest {
    @Test
    void signalBankTransferWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferWorkflowApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                BudgetServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                TicketQueryServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new BankTransferWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                BankTransferQueryServiceGrpc.newBlockingStub(channel),
                BankTransferReportingServiceGrpc.newBlockingStub(channel),
                FxHedgeApprovalServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SignalBankTransferWorkflowRequest.getDefaultInstance());
        var response = application.signalBankTransferWorkflow(request);
        var sent0 = channel.request("acme.treasury.v1.LiquidityForecastImportService/ListLiquidityForecastImports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "attachments", "file_name"),
                ProbeSupport.value(sent1, "posting_batch", "attachments", "file_name"));
        var sent2 = channel.request("acme.treasury.v1.BankTransferQueryService/SuggestBankTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility"), "posting_batch", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility"), "posting_batch", "counterparty", "ledger_account_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility"), "posting_batch", "metadata"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.treasury.v1.BankTransferReportingService/CountBankTransferTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility"), "posting_batch", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "lines", "line_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "lines", "attributes"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility"), "posting_batch", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastImportService/ListLiquidityForecastImports"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferQueryService/SuggestBankTransfers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "issued_at"),
                ProbeSupport.value(response, "bank_transfer", "issued_at"));
    }
}
