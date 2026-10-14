package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.localization.v1.ExplainTranslationKeyCalculationRequest;
import com.acme.localization.v1.ExplainTranslationKeyCalculationResponse;
import com.acme.localization.v1.TranslationKey;
import com.acme.localization.v1.TranslationKeyCalculationServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteBankSettlement;
import com.acme.routing.v1.RouteCalculationServiceGrpc;
import com.acme.routing.v1.SimulateRouteRequest;
import com.acme.routing.v1.SimulateRouteResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CalculateBankTransferRequest;
import com.acme.treasury.v1.CalculateBankTransferResponse;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.EstimateBankTransferRequest;
import com.acme.treasury.v1.EstimateBankTransferResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetBankTransferRatesRequest;
import com.acme.treasury.v1.GetBankTransferRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.NormalizeVenueRequest;
import com.acme.venues.v1.NormalizeVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BankTransferCalculationApplicationTest {
    @Test
    void recalculateBankTransfersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferCalculationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                TranslationKeyCalculationServiceGrpc.newBlockingStub(channel),
                new BankTransferCalculationRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                RouteCalculationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecalculateBankTransfersRequest.getDefaultInstance());
        var response = application.recalculateBankTransfers(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "number"),
                ProbeSupport.value(sent0, "audit_event", "subject", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "counterparty", "email"),
                ProbeSupport.value(sent0, "details", "author_email"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetLineValidationService/NormalizeBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "attachments", "checksum"),
                ProbeSupport.value(sent1, "budget_line", "attachments", "checksum"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/CompareLedgerAccountVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineValidationService/NormalizeBudgetLine"), "budget_line", "audit", "etag"),
                ProbeSupport.value(sent2, "ledger_account", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "updated_at"),
                ProbeSupport.value(sent2, "ledger_account", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "counterparty", "email"),
                ProbeSupport.value(sent2, "ledger_account", "contact", "email"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetLineValidationService/ScoreBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/CompareLedgerAccountVersions"), "ledger_account", "holder", "display_name"),
                ProbeSupport.value(sent3, "budget_line", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "budget_line", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineValidationService/NormalizeBudgetLine"), "budget_line", "audit", "etag"),
                ProbeSupport.value(sent3, "budget_line", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/CompareLedgerAccountVersions"), "ledger_account", "audit", "etag"),
                ProbeSupport.value(response, "bank_transfer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineValidationService/ScoreBudgetLine"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "metadata"),
                ProbeSupport.value(response, "bank_transfer", "metadata"));
    }
}
