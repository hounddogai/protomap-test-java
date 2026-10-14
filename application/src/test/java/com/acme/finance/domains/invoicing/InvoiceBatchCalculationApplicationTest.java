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
import com.acme.finance.testing.ProbeSupport;
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
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EstimateInvoiceBatchRequest;
import com.acme.invoicing.v1.EstimateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceTemplate;
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
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.acme.sponsorships.v1.SponsorNationalId;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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

class InvoiceBatchCalculationApplicationTest {
    @Test
    void simulateInvoiceBatchPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchCalculationApplicationEvents(SponsorCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchCalculationApplication.QuoteInvoiceBatchStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchCalculationApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                MileageClaimValidationServiceGrpc.newBlockingStub(channel),
                FuelCardQueryServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureCalculationServiceGrpc.newFutureStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SimulateInvoiceBatchRequest.getDefaultInstance());
        var response = application.simulateInvoiceBatch(request);
        var sent0 = channel.request("acme.banking.v1.MandateCalculationService/EstimateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "recipient"),
                ProbeSupport.value(sent0, "mandate", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "etag"),
                ProbeSupport.value(sent0, "mandate", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "mandate", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "counterparty", "customer_id"),
                ProbeSupport.value(sent0, "mandate", "counterparty", "customer_id"));
        var sent1 = channel.request("acme.risk.v1.ExposureCalculationService/CalculateExposure");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "mandate", "audit", "created_at"),
                ProbeSupport.value(sent1, "exposure", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "card", "token"),
                ProbeSupport.value(sent1, "exposure", "card", "token"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetService/CreateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "fixed_asset", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "locality"),
                ProbeSupport.value(sent2, "fixed_asset", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "mandate", "audit", "updated_at"),
                ProbeSupport.value(sent2, "fixed_asset", "audit", "updated_at"));
        var sent3 = channel.request("acme.audit.v1.AuditEventExportService/ListAuditEventExports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "mandate", "counterparty", "customer_id"),
                ProbeSupport.value(response, "invoice_batch", "counterparty", "customer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CalculateExposure"), "exposure", "audit", "etag"),
                ProbeSupport.value(response, "invoice_batch", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetService/CreateFixedAsset"), "fixed_asset", "components", "description"),
                ProbeSupport.value(response, "invoice_batch", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventExportService/ListAuditEventExports"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch_id"),
                ProbeSupport.value(response, "invoice_batch", "invoice_batch_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "notes"),
                ProbeSupport.value(response, "invoice_batch", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceBatchCalculationApplicationEvents(SponsorCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculateSponsorResponse>();
        listener.on(new InvoiceBatchCalculationApplication.QuoteInvoiceBatchStage1Requested(ProbeSupport.sample(CalculateSponsorRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
