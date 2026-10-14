package com.acme.finance.domains.procurement;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateAttachment;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fuel.v1.CountFuelCardsRequest;
import com.acme.fuel.v1.CountFuelCardsResponse;
import com.acme.fuel.v1.EstimateFuelCardRequest;
import com.acme.fuel.v1.EstimateFuelCardResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardBankFunding;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.fuel.v1.FuelCardLimit;
import com.acme.fuel.v1.FuelCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierNationalId;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.webhooks.v1.FindWebhookEndpointsRequest;
import com.acme.webhooks.v1.FindWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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

class PurchaseRequisitionCalculationApplicationTest {
    @Test
    void estimatePurchaseRequisitionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionCalculationApplication(
                FixedAssetServiceGrpc.newStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                CertificateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                FuelCardCalculationServiceGrpc.newBlockingStub(channel),
                FuelCardQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                new PurchaseRequisitionCalculationRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierServiceGrpc.newBlockingStub(channel),
                WebhookEndpointQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EstimatePurchaseRequisitionRequest.getDefaultInstance());
        var response = application.estimatePurchaseRequisition(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryValidationService/PreviewJournalEntry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "lines", "tax", "exemptions"),
                ProbeSupport.value(sent0, "journal_entry", "lines", "tax", "exemptions"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/CompareBudgetLineOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "updated_at"),
                ProbeSupport.value(sent1, "budget_line", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/PreviewJournalEntry"), "journal_entry", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "budget_line", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "bank", "bic"),
                ProbeSupport.value(sent1, "budget_line", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address", "lines"),
                ProbeSupport.value(sent1, "budget_line", "billing_address", "lines"));
        var sent2 = channel.request("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/PreviewJournalEntry"), "journal_entry", "card", "network"),
                ProbeSupport.value(sent2, "mandate", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CompareBudgetLineOptions"), "budget_line", "audit", "updated_by"),
                ProbeSupport.value(sent2, "mandate", "audit", "updated_by"));
        var sent3 = channel.request("acme.webhooks.v1.WebhookEndpointQueryService/FindWebhookEndpoints");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "lines", "line_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CompareBudgetLineOptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition_id"),
                ProbeSupport.value(response, "purchase_requisition", "purchase_requisition_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "metadata"),
                ProbeSupport.value(response, "purchase_requisition", "metadata"));
    }
}
