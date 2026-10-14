package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.expenses.v1.ApproveMileageClaimRequest;
import com.acme.expenses.v1.ApproveMileageClaimResponse;
import com.acme.expenses.v1.EscalateMileageClaimRequest;
import com.acme.expenses.v1.EscalateMileageClaimResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.MileageClaimBankSettlement;
import com.acme.expenses.v1.MileageClaimCardSettlement;
import com.acme.expenses.v1.SubmitMileageClaimRequest;
import com.acme.expenses.v1.SubmitMileageClaimResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.RecallCoverageRequest;
import com.acme.insurance.v1.RecallCoverageResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowRequest;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferApprovalServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import com.acme.treasury.v1.CancelFxHedgeWorkflowRequest;
import com.acme.treasury.v1.CancelFxHedgeWorkflowResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CommentBankTransferApprovalRequest;
import com.acme.treasury.v1.CommentBankTransferApprovalResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.InspectFxHedgeWorkflowRequest;
import com.acme.treasury.v1.InspectFxHedgeWorkflowResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.ListBankTransferReportsRequest;
import com.acme.treasury.v1.ListBankTransferReportsResponse;
import com.acme.treasury.v1.ListFxHedgeWorkflowsRequest;
import com.acme.treasury.v1.ListFxHedgeWorkflowsResponse;
import com.acme.treasury.v1.PauseFxHedgeWorkflowRequest;
import com.acme.treasury.v1.PauseFxHedgeWorkflowResponse;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowRequest;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowResponse;
import com.acme.treasury.v1.RetryFxHedgeWorkflowRequest;
import com.acme.treasury.v1.RetryFxHedgeWorkflowResponse;
import com.acme.treasury.v1.SignalBankTransferWorkflowRequest;
import com.acme.treasury.v1.SignalBankTransferWorkflowResponse;
import com.acme.treasury.v1.SignalFxHedgeWorkflowRequest;
import com.acme.treasury.v1.SignalFxHedgeWorkflowResponse;
import com.acme.treasury.v1.StartFxHedgeWorkflowRequest;
import com.acme.treasury.v1.StartFxHedgeWorkflowResponse;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FxHedgeWorkflowApplicationTest {
    @Test
    void signalFxHedgeWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeWorkflowApplication(
                AuditEventServiceGrpc.newStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimApprovalServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferApprovalServiceGrpc.newBlockingStub(channel),
                BankTransferReportingServiceGrpc.newBlockingStub(channel),
                BankTransferWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SignalFxHedgeWorkflowRequest.getDefaultInstance());
        var response = application.signalFxHedgeWorkflow(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "subscriber", "phone"));
        var sent1 = channel.request("acme.customers.v1.HouseholdQueryService/FindHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "audit", "etag"),
                ProbeSupport.value(sent2, "preference", "audit", "etag"));
        var sent3 = channel.request("acme.notifications.v1.NotificationQueryService/FindNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/FindHouseholds"), "result", "details"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences"), "preference", "owner", "user_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/FindHouseholds"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences"), "preference", "owner", "display_name"),
                ProbeSupport.value(response, "fx_hedge", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/FindNotifications"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/FindNotifications"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "due_at"),
                ProbeSupport.value(response, "fx_hedge", "due_at"));
    }
}
