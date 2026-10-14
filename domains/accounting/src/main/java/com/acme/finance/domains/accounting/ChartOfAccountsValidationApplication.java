package com.acme.finance.domains.accounting;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks chart of accountses against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ChartOfAccountsValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ChartOfAccountsValidationApplication.class);

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FormServiceGrpc.FormServiceBlockingStub formsFormServiceBlocking;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking;

    private final ChartOfAccountsValidationRequestMapper mapper;

    private final MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PipelineCalculationServiceGrpc.PipelineCalculationServiceBlockingStub pipelinesPipelineCalculationServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceFutureStub procurementGoodsReceiptValidationServiceFuture;

    private final AccountingRepository repository;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public ChartOfAccountsValidationApplication(
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking,
            ApplicationEventPublisher events,
            FormServiceGrpc.FormServiceBlockingStub formsFormServiceBlocking,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking,
            ChartOfAccountsValidationRequestMapper mapper,
            MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PipelineCalculationServiceGrpc.PipelineCalculationServiceBlockingStub pipelinesPipelineCalculationServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceFutureStub procurementGoodsReceiptValidationServiceFuture,
            AccountingRepository repository,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseServiceBlocking = complianceComplianceCaseServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.driversDriverServiceBlocking = driversDriverServiceBlocking;
        this.events = events;
        this.formsFormServiceBlocking = formsFormServiceBlocking;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.localizationTranslationKeyQueryServiceBlocking = localizationTranslationKeyQueryServiceBlocking;
        this.mapper = mapper;
        this.mediaMediaAssetServiceBlocking = mediaMediaAssetServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.pipelinesPipelineCalculationServiceBlocking = pipelinesPipelineCalculationServiceBlocking;
        this.procurementGoodsReceiptValidationServiceFuture = procurementGoodsReceiptValidationServiceFuture;
        this.repository = repository;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    public record ExplainChartOfAccountsDecisionStage1Requested(ExplainAuditEventDecisionRequest request, CompletableFuture<ExplainAuditEventDecisionResponse> completion, io.grpc.Context context) {}

    public record ExplainChartOfAccountsDecisionStage2Requested(ListJournalEntryVersionsRequest request, CompletableFuture<ListJournalEntryVersionsResponse> completion, io.grpc.Context context) {}

    public record ScoreChartOfAccountsStage2Requested(ScoreAuditTrailRequest request, CompletableFuture<ScoreAuditTrailResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a chart of accounts.
     */
    public ValidateChartOfAccountsResponse validateChartOfAccounts(ValidateChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = mapper.validateChartOfAccountsStage0(request);
        var reply0 = complianceComplianceCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listComplianceCases(outbound0);

        var outbound1 = CheckGoodsReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getChartOfAccounts().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var pending1 = procurementGoodsReceiptValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkGoodsReceiptEligibility(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BatchGetDriversRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage2Adapter {
            BatchGetDriversResponse fetch(BatchGetDriversRequest input) {
                return driversDriverServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDrivers(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(BatchGetLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .build()), BatchGetLoanApplicationsRequest.class);
        var reply3 = lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetLoanApplications(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return ValidateChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getGoodsReceipt().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .setStatus(request.getChartOfAccounts().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a chart of accounts.
     */
    public PreviewChartOfAccountsResponse previewChartOfAccounts(PreviewChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = mapper.previewChartOfAccountsStage0(request);
        var reply0 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDisposal(outbound0);

        var outbound1 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDisposal().getAssignee().getEmail())
                                .setPhone(reply0.getDisposal().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.LedgerAccountHistoryService");
        var selected1 = LedgerAccountHistoryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.annotateLedgerAccountVersion(outbound1);

        var outbound2 = EstimateDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getDisposal().getRequester().getName())
                                .build())
                        .build())
                .build();
        RpcPort<EstimateDisposalRequest, EstimateDisposalResponse> port2 = RpcPort.adapt(assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateDisposal);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return PreviewChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getDisposal().getAudit().getUpdatedBy())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setUserId(reply0.getDisposal().getAssignee().getUserId())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply2.getDisposal().getAssignee().getDisplayName())
                                .build())
                        .setName(request.getChartOfAccounts().getName())
                        .build())
                .setOutcome(PreviewChartOfAccountsResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Checks a chart of accounts.
     */
    public CheckChartOfAccountsEligibilityResponse checkChartOfAccountsEligibility(CheckChartOfAccountsEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckChartOfAccountsEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = FindTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindTranslationKeysRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChartOfAccounts().getAudit().getCreatedAt())
                        .setOwnerId(request.getChartOfAccounts().getManager().getUserId())
                        .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> localizationTranslationKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findTranslationKeys(value));

        var outbound1 = EstimatePipelineRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePipelineRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getChartOfAccounts().getLocation().getCountryCode())
                                .build())
                        .build())
                .setPipeline(Pipeline.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(request.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .addParticipants(PipelineParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getContact().getEmail())
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> pipelinesPipelineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePipeline(value));

        var outbound2 = CheckAuditEventEligibilityRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckAuditEventEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<CheckAuditEventEligibilityRequest, CheckAuditEventEligibilityResponse>() {
            @Override
            public CheckAuditEventEligibilityResponse apply(CheckAuditEventEligibilityRequest input) {
                return auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAuditEventEligibility(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = SubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<SubscribeJournalEntryUpdatesRequest, SubscribeJournalEntryUpdatesResponse>() {
            @Override
            public SubscribeJournalEntryUpdatesResponse apply(SubscribeJournalEntryUpdatesRequest input) {
                return ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckChartOfAccountsEligibility", request.getChartOfAccounts().getChartOfAccountsId());
        return CheckChartOfAccountsEligibilityResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply1.getPipeline().getLocation().getLocality())
                                .setRegion(reply1.getPipeline().getLocation().getRegion())
                                .build())
                        .build())
                .setOutcome(CheckChartOfAccountsEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a chart of accounts.
     */
    public NormalizeChartOfAccountsResponse normalizeChartOfAccounts(NormalizeChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = (CalculateDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateDisposalRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getChartOfAccounts().getLocation().getCountryCode())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateDisposal(outbound0);

        var outbound1 = WatchMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchMediaAssetsRequest.Filter.newBuilder()
                        .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getDisposal().getAssignee().getEmail())
                        .setName(request.getDetails().getContact().getName())
                        .setPhone(request.getDetails().getContact().getPhone())
                        .build())
                .build();
        var stream1 = mediaMediaAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMediaAssets(outbound1);
        var reply1 = WatchMediaAssetsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = mapper.normalizeChartOfAccountsStage2(request);
        var reply2 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateEInvoice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return NormalizeChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getDisposal().getAudit().getUpdatedBy())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(reply2.getEInvoice().getBillingAddress().getLinesList())
                                        .build())
                                .build())
                        .setStatus(request.getChartOfAccounts().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a chart of accounts.
     */
    public ExplainChartOfAccountsDecisionResponse explainChartOfAccountsDecision(ExplainChartOfAccountsDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainChartOfAccountsDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = UnsubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeFiscalPeriodUpdates(outbound0);

        var outbound1 = ExplainAuditEventDecisionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAuditEventDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ExplainAuditEventDecisionResponse>();
        events.publishEvent(new ExplainChartOfAccountsDecisionStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = ListJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntryVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .setRegion(reply0.getFiscalPeriod().getLocation().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ListJournalEntryVersionsResponse>();
        events.publishEvent(new ExplainChartOfAccountsDecisionStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = NormalizeBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getFiscalPeriod().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeBudgetRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            NormalizeBudgetResponse fetch(NormalizeBudgetRequest input) {
                return budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBudget(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainChartOfAccountsDecision", request.getChartOfAccounts().getChartOfAccountsId());
        return ExplainChartOfAccountsDecisionResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .putAllAttributes(request.getChartOfAccounts().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getFiscalPeriod().getLocation().getLinesList())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply1.getAuditEvent().getSubject().getDisplayName())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply1.getAuditEvent().getSubject().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Scores a chart of accounts.
     */
    public ScoreChartOfAccountsResponse scoreChartOfAccounts(ScoreChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = ExportTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportTranslationKeysRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChartOfAccounts().getAudit().getUpdatedAt())
                        .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var stream0 = localizationTranslationKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportTranslationKeys(outbound0);
        var reply0 = ExportTranslationKeysResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BeanCopies.copy(CloneFormRequest.newBuilder()
                .setContext(request.getContext())
                .setForm(Form.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getChartOfAccounts().getLocation().getLinesList())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .build(), CloneFormRequest.class);
        var reply1 = formsFormServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneForm(outbound1);

        var outbound2 = ScoreAuditTrailRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreAuditTrailRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getChartOfAccounts().getLocation().getLocality())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ScoreAuditTrailResponse>();
        events.publishEvent(new ScoreChartOfAccountsStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = LintRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintRiskScenariosRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getAuditTrail().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getForm().getAudit().getCreatedBy())
                                .setUpdatedBy(reply2.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.risk.v1.RiskScenarioValidationService");
        var selected3 = RiskScenarioValidationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.lintRiskScenarios(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return ScoreChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(reply3.getRiskScenario().getAudit().getUpdatedBy())
                                        .build())
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply2.getAuditTrail().getBillingAddress().getCountryCode())
                                                .build())
                                        .build())
                                .build())
                        .setName(request.getChartOfAccounts().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints a chart of accounts.
     */
    public LintChartOfAccountsesResponse lintChartOfAccountses(LintChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        Function<NormalizeAuditEventRequest, NormalizeAuditEventResponse> operation0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeAuditEvent;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = NormalizeTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeTaxWithholdingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeTaxWithholdingRequest, NormalizeTaxWithholdingResponse> port1 = RpcPort.adapt(payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeTaxWithholding);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .addLines(SupplierRatingLine.newBuilder()
                                .setTax(SupplierRatingLineTax.newBuilder()
                                        .setAmount(reply1.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            PreviewSupplierRatingResponse fetch(PreviewSupplierRatingRequest input) {
                return suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRating(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = mapper.lintChartOfAccountsesStage3(request, reply2);
        var reply3 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewControl(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintChartOfAccountses", request.getChartOfAccounts().getChartOfAccountsId());
        return LintChartOfAccountsesResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getTaxWithholding().getAudit().getCreatedAt())
                                .setUpdatedBy(reply2.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(reply2.getSupplierRating().getBillingAddress().getLinesList())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                        .build())
                                .build())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply3.getControl().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(LintChartOfAccountsesResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a chart of accounts.
     */
    public GetChartOfAccountsRulesResponse getChartOfAccountsRules(GetChartOfAccountsRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetChartOfAccountsRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "GetChartOfAccountsRules");

        var outbound0 = CloneLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLoanApplication(value));
        return GetChartOfAccountsRulesResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
