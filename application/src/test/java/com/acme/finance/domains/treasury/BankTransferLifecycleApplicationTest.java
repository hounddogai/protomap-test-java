package com.acme.finance.domains.treasury;

import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.RejectForecastRequest;
import com.acme.budgeting.v1.RejectForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.localization.v1.BatchCreateTranslationKeysRequest;
import com.acme.localization.v1.BatchCreateTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyServiceGrpc;
import com.acme.modelregistry.v1.CountRegisteredModelsRequest;
import com.acme.modelregistry.v1.CountRegisteredModelsResponse;
import com.acme.modelregistry.v1.RegisteredModelQueryServiceGrpc;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.timekeeping.v1.CountTimesheetsRequest;
import com.acme.timekeeping.v1.CountTimesheetsResponse;
import com.acme.timekeeping.v1.TimesheetQueryServiceGrpc;
import com.acme.treasury.v1.ActivateBankTransferRequest;
import com.acme.treasury.v1.ActivateBankTransferResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireBankTransferRequest;
import com.acme.treasury.v1.ExpireBankTransferResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FetchBankTransferReportRequest;
import com.acme.treasury.v1.FetchBankTransferReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.GetBankTransferLifecycleRequest;
import com.acme.treasury.v1.GetBankTransferLifecycleResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ReinstateBankTransferRequest;
import com.acme.treasury.v1.ReinstateBankTransferResponse;
import com.acme.treasury.v1.RenewBankTransferRequest;
import com.acme.treasury.v1.RenewBankTransferResponse;
import com.acme.treasury.v1.ScheduleBankTransferExpiryRequest;
import com.acme.treasury.v1.ScheduleBankTransferExpiryResponse;
import com.acme.treasury.v1.SummarizeCashPositionsRequest;
import com.acme.treasury.v1.SummarizeCashPositionsResponse;
import com.acme.treasury.v1.SuspendBankTransferRequest;
import com.acme.treasury.v1.SuspendBankTransferResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateBankTransferRequest;
import com.acme.treasury.v1.TerminateBankTransferResponse;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BankTransferLifecycleApplicationTest {
    @Test
    void terminateBankTransferPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferLifecycleApplication(
                ForecastApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                TranslationKeyServiceGrpc.newStub(channel),
                new BankTransferLifecycleRequestMapperImpl(),
                RegisteredModelQueryServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                TimesheetQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferReportingServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionQueryServiceGrpc.newBlockingStub(channel),
                FxHedgeReportingServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateBankTransferRequest.getDefaultInstance());
        var response = application.terminateBankTransfer(request);
        var sent0 = channel.request("acme.venues.v1.VenueValidationService/ExplainVenueDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "venue", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "venue", "owner", "email"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferReportingService/FetchBankTransferReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(sent1, "bank_transfer_id"));
        var sent2 = channel.request("acme.insurance.v1.PremiumReportingService/CountPremiumTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/FetchBankTransferReport"), "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ExplainVenueDecision"), "venue", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ExplainVenueDecision"), "venue", "components", "component_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.budgeting.v1.ForecastApprovalService/RejectForecast");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/FetchBankTransferReport"), "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent3, "forecast", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/FetchBankTransferReport"), "bank_transfer", "billing_address", "locality"),
                ProbeSupport.value(sent3, "forecast", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ExplainVenueDecision"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ExplainVenueDecision"), "venue", "owner", "display_name"),
                ProbeSupport.value(response, "bank_transfer", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/FetchBankTransferReport"), "bank_transfer", "billing_address"),
                ProbeSupport.value(response, "bank_transfer", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/CountPremiumTotals"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/CountPremiumTotals"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastApprovalService/RejectForecast"), "forecast", "metadata"),
                ProbeSupport.value(response, "bank_transfer", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
