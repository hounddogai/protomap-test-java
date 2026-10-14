package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.QuoteUnderwritingRequest;
import com.acme.insurance.v1.QuoteUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.maintenance.v1.SimulateWorkOrderRequest;
import com.acme.maintenance.v1.SimulateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.scheduling.v1.CountShiftsRequest;
import com.acme.scheduling.v1.CountShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckBankTransferEligibilityRequest;
import com.acme.treasury.v1.CheckBankTransferEligibilityResponse;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.EstimateBankTransferRequest;
import com.acme.treasury.v1.EstimateBankTransferResponse;
import com.acme.treasury.v1.ExplainBankTransferDecisionRequest;
import com.acme.treasury.v1.ExplainBankTransferDecisionResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeLineTax;
import com.acme.treasury.v1.GetBankTransferRatesRequest;
import com.acme.treasury.v1.GetBankTransferRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.NormalizeBankTransferRequest;
import com.acme.treasury.v1.NormalizeBankTransferResponse;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.ValidateBankTransferRequest;
import com.acme.treasury.v1.ValidateBankTransferResponse;
import java.util.List;
import java.util.Optional;
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

class BankTransferValidationApplicationTest {
    @Test
    void validateBankTransferPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new BankTransferValidationApplicationEvents(WorkOrderCalculationServiceGrpc.newStub(channel), MetricSeriesQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BankTransferValidationApplication.ExplainBankTransferDecisionStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof BankTransferValidationApplication.ScoreBankTransferStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BankTransferValidationApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                ShiftQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateBankTransferRequest.getDefaultInstance());
        var response = application.validateBankTransfer(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingCalculationService/QuoteUnderwriting");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent0, "underwriting", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "created_at"),
                ProbeSupport.value(sent0, "underwriting", "comments", "created_at"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/QuoteUnderwriting"), "underwriting", "audit", "updated_by"),
                ProbeSupport.value(sent1, "cash_position", "audit", "updated_by"));
        var sent2 = channel.request("acme.audit.v1.AuditEventExportService/ListAuditEventExports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "cash_position", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "cash_position", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "result", "correlation_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.treasury.v1.BankTransferCalculationService/EstimateBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "created_at"),
                ProbeSupport.value(sent3, "bank_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "bank", "iban"),
                ProbeSupport.value(sent3, "bank_transfer", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "bank_transfer", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/QuoteUnderwriting"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventExportService/ListAuditEventExports"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventExportService/ListAuditEventExports"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "total"),
                ProbeSupport.value(response, "bank_transfer", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BankTransferValidationApplicationEvents(WorkOrderCalculationServiceGrpc.newStub(channel), MetricSeriesQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SimulateWorkOrderResponse>();
        listener.on(new BankTransferValidationApplication.ExplainBankTransferDecisionStage0Requested(ProbeSupport.sample(SimulateWorkOrderRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<BrowseMetricSeriesesResponse>();
        listener.on(new BankTransferValidationApplication.ScoreBankTransferStage1Requested(ProbeSupport.sample(BrowseMetricSeriesesRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
