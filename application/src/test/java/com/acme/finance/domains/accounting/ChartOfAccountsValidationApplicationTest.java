package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.GetChartOfAccountsRulesRequest;
import com.acme.accounting.v1.GetChartOfAccountsRulesResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.assets.v1.CalculateDisposalRequest;
import com.acme.assets.v1.CalculateDisposalResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.NormalizeBudgetRequest;
import com.acme.budgeting.v1.NormalizeBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.drivers.v1.BatchGetDriversRequest;
import com.acme.drivers.v1.BatchGetDriversResponse;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.CloneFormRequest;
import com.acme.forms.v1.CloneFormResponse;
import com.acme.forms.v1.Form;
import com.acme.forms.v1.FormServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.localization.v1.ExportTranslationKeysRequest;
import com.acme.localization.v1.ExportTranslationKeysResponse;
import com.acme.localization.v1.FindTranslationKeysRequest;
import com.acme.localization.v1.FindTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.media.v1.MediaAssetServiceGrpc;
import com.acme.media.v1.WatchMediaAssetsRequest;
import com.acme.media.v1.WatchMediaAssetsResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.pipelines.v1.EstimatePipelineRequest;
import com.acme.pipelines.v1.EstimatePipelineResponse;
import com.acme.pipelines.v1.Pipeline;
import com.acme.pipelines.v1.PipelineCalculationServiceGrpc;
import com.acme.pipelines.v1.PipelineParticipant;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ChartOfAccountsValidationApplicationTest {
    @Test
    void validateChartOfAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new ChartOfAccountsValidationApplicationEvents(AuditEventValidationServiceGrpc.newStub(channel), AuditTrailValidationServiceGrpc.newStub(channel), JournalEntryHistoryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof ChartOfAccountsValidationApplication.ScoreChartOfAccountsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ChartOfAccountsValidationApplication(
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                DriverServiceGrpc.newBlockingStub(channel),
                events,
                FormServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                TranslationKeyQueryServiceGrpc.newBlockingStub(channel),
                new ChartOfAccountsValidationRequestMapperImpl(),
                MediaAssetServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                PipelineCalculationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newFutureStub(channel),
                repository,
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateChartOfAccountsRequest.getDefaultInstance());
        var response = application.validateChartOfAccounts(request);
        var sent0 = channel.request("acme.compliance.v1.ComplianceCaseService/ListComplianceCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "attributes"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.procurement.v1.GoodsReceiptValidationService/CheckGoodsReceiptEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "created_by"),
                ProbeSupport.value(sent1, "goods_receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "recipient"),
                ProbeSupport.value(sent1, "goods_receipt", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "updated_by"),
                ProbeSupport.value(sent1, "goods_receipt", "audit", "updated_by"));
        var sent2 = channel.request("acme.drivers.v1.DriverService/BatchGetDrivers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.lending.v1.LoanApplicationService/BatchGetLoanApplications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseService/ListComplianceCases"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseService/ListComplianceCases"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/CheckGoodsReceiptEligibility"), "goods_receipt", "audit", "created_at"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationService/BatchGetLoanApplications"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts_id"),
                ProbeSupport.value(response, "chart_of_accounts", "chart_of_accounts_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "status"),
                ProbeSupport.value(response, "chart_of_accounts", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ChartOfAccountsValidationApplicationEvents(AuditEventValidationServiceGrpc.newStub(channel), AuditTrailValidationServiceGrpc.newStub(channel), JournalEntryHistoryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainAuditEventDecisionResponse>();
        listener.on(new ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage1Requested(ProbeSupport.sample(ExplainAuditEventDecisionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ListJournalEntryVersionsResponse>();
        listener.on(new ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage2Requested(ProbeSupport.sample(ListJournalEntryVersionsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<ScoreAuditTrailResponse>();
        listener.on(new ChartOfAccountsValidationApplication.ScoreChartOfAccountsStage2Requested(ProbeSupport.sample(ScoreAuditTrailRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
