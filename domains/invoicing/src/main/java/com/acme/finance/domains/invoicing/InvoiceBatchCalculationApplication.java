package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferLineTax;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAttachment;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimBankSettlement;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.RecalculateExpensePoliciesRequest;
import com.acme.expenses.v1.RecalculateExpensePoliciesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fuel.v1.FuelCardQueryServiceGrpc;
import com.acme.fuel.v1.ResolveFuelCardsRequest;
import com.acme.fuel.v1.ResolveFuelCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.EstimateInvoiceBatchRequest;
import com.acme.invoicing.v1.EstimateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.SimulateInvoiceBatchRequest;
import com.acme.invoicing.v1.SimulateInvoiceBatchResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.sponsorships.v1.CalculateSponsorRequest;
import com.acme.sponsorships.v1.CalculateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorNationalId;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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
 * Computes the figures of an invoice batch, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class InvoiceBatchCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchCalculationApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking;

    private final MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking;

    private final FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final InvoicingRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceFutureStub riskExposureCalculationServiceFuture;

    private final Executor rpcExecutor;

    public InvoiceBatchCalculationApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking,
            MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking,
            FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            InvoicingRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceFutureStub riskExposureCalculationServiceFuture,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.expensesExpensePolicyCalculationServiceBlocking = expensesExpensePolicyCalculationServiceBlocking;
        this.expensesMileageClaimValidationServiceBlocking = expensesMileageClaimValidationServiceBlocking;
        this.fuelFuelCardQueryServiceBlocking = fuelFuelCardQueryServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.repository = repository;
        this.riskExposureCalculationServiceFuture = riskExposureCalculationServiceFuture;
        this.rpcExecutor = rpcExecutor;
    }

    public record QuoteInvoiceBatchStage1Requested(CalculateSponsorRequest request, CompletableFuture<CalculateSponsorResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an invoice batch.
     */
    public CalculateInvoiceBatchResponse calculateInvoiceBatch(CalculateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EstimateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoiceBatch().getAudit().getCreatedBy())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateClaim(outbound0);

        var outbound1 = StreamPostingBatchNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getClaim().getCustomer().getEmail())
                        .setName(reply0.getClaim().getRequester().getName())
                        .build())
                .build();
        var stream1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPostingBatchNotices(outbound1);
        var reply1 = StreamPostingBatchNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getCustomer().getEmail())
                                .setName(reply0.getClaim().getRequester().getName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.ledger.v1.PostingBatchNotificationService");
        var selected2 = PostingBatchNotificationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.subscribePostingBatchUpdates(outbound2);

        var futures3 = request.getInvoiceBatch().getAttachmentsList().stream()
                .map(element -> CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addAttachments(AuditTrailAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? CompareAuditTrailVersionsResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return CalculateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getPostingBatch().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getClaim().getCustomer().getCustomerId())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Estimates an invoice batch.
     */
    public EstimateInvoiceBatchResponse estimateInvoiceBatch(EstimateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = RecalculateExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateExpensePoliciesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateExpensePolicies(value));

        var outbound1 = ResolveFuelCardsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            ResolveFuelCardsResponse fetch(ResolveFuelCardsRequest input) {
                return fuelFuelCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveFuelCards(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = BeanCopies.copy(NormalizeAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .addLines(AssetTransferLine.newBuilder()
                                .setTax(AssetTransferLineTax.newBuilder()
                                        .setAmount(reply0.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAssetTransferRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .build(), NormalizeAssetTransferRequest.class);
        var reply2 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAssetTransfer(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return EstimateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getExpensePolicy().getOwner().getDisplayName())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes an invoice batch.
     */
    public QuoteInvoiceBatchResponse quoteInvoiceBatch(QuoteInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = PreviewBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setToken(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudget(outbound0);

        var outbound1 = CalculateSponsorRequest.newBuilder()
                .setContext(request.getContext())
                .setSponsor(Sponsor.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getBudget().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getBudget().getContact().getName())
                                .build())
                        .setNationalId(SponsorNationalId.newBuilder()
                                .setNumber(reply0.getBudget().getHolder().getNumber())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CalculateSponsorResponse>();
        events.publishEvent(new QuoteInvoiceBatchStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return QuoteInvoiceBatchResponse.newBuilder()
                .setBreakdown(QuoteInvoiceBatchResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setStatus(request.getInvoiceBatch().getStatus())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Compares an invoice batch.
     */
    public CompareInvoiceBatchOptionsResponse compareInvoiceBatchOptions(CompareInvoiceBatchOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareInvoiceBatchOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = AnnotateAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceBatch().getAudit().getEtag())
                                .setUpdatedBy(request.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setHolderName(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditTrailVersion(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareInvoiceBatchOptions", request.getInvoiceBatch().getInvoiceBatchId());
        return CompareInvoiceBatchOptionsResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCounterparty(request.getInvoiceBatch().getCounterparty())
                        .setDueAt(reply0.getAuditTrail().getDueAt())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .build();
    }

    /**
     * Recalculates an invoice batch.
     */
    public RecalculateInvoiceBatchesResponse recalculateInvoiceBatches(RecalculateInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = JsonCodec.roundTrip(RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateClaimsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build(), RecalculateClaimsRequest.class);
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateClaims(outbound0);

        var outbound1 = PreviewMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBank(MileageClaimBankSettlement.newBuilder()
                                .setBic(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case BANK -> request.getInvoiceBatch().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMileageClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateInvoiceBatches", request.getInvoiceBatch().getInvoiceBatchId());
        return RecalculateInvoiceBatchesResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getClaim().getAudit().getEtag())
                                .build())
                        .setBillingAddress(reply1.getMileageClaim().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Simulates an invoice batch.
     */
    public SimulateInvoiceBatchResponse simulateInvoiceBatch(SimulateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(request.getInvoiceBatch().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(value)).orElseThrow();

        var outbound1 = CalculateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMandate().getAudit().getCreatedAt())
                                .build())
                        .setCard(ExposureCardFunding.newBuilder()
                                .setToken(request.getInvoiceBatch().getCard().getToken())
                                .build())
                        .build())
                .build();
        var pending1 = riskExposureCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExposure(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = CreateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        Function<CreateFixedAssetRequest, CreateFixedAssetResponse> operation2 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createFixedAsset;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = ListAuditEventExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventExportsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.audit.v1.AuditEventExportService/ListAuditEventExports", outbound3);
        var reply3 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventExports(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return SimulateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getExposure().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getMandate().getCounterparty().getCustomerId())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .addLines(InvoiceBatchLine.newBuilder()
                                .setDescription(reply2.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .setNotes(request.getInvoiceBatch().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains an invoice batch.
     */
    public ExplainInvoiceBatchCalculationResponse explainInvoiceBatchCalculation(ExplainInvoiceBatchCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceBatchCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = RecalculateClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateClaimsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        Function<RecalculateClaimsRequest, RecalculateClaimsResponse> operation0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateClaims;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = BrowseLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceBatch().getAudit().getCreatedAt())
                        .setOwnerId(switch (reply0.getClaim().getOriginCase()) {
                            case CUSTOMER -> reply0.getClaim().getCustomer().getCustomerId();
                            default -> "";
                        })
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLoanApplications(outbound1);

        var outbound2 = WatchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchFixedAssetsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                        .setName(reply0.getClaim().getRequester().getName())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFixedAssets(outbound2);
        var reply2 = WatchFixedAssetsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateMandateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            EstimateMandateResponse fetch(EstimateMandateRequest input) {
                return bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceBatchCalculation", request.getInvoiceBatch().getInvoiceBatchId());
        return ExplainInvoiceBatchCalculationResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getClaim().getAudit().getEtag())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .setOutcome(ExplainInvoiceBatchCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an invoice batch.
     */
    public GetInvoiceBatchRatesResponse getInvoiceBatchRates(GetInvoiceBatchRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceBatchRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "GetInvoiceBatchRates");

        var outbound0 = ValidateAssetTransferRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateAssetTransferRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAssetTransfer(outbound0);
        return GetInvoiceBatchRatesResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(reply0.getAssetTransfer().getMetadataMap())
                        .build())
                .build();
    }
}
