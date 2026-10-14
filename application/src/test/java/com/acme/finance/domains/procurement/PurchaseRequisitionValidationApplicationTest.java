package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.RecordJournalEntryVersionRequest;
import com.acme.ledger.v1.RecordJournalEntryVersionResponse;
import com.acme.localization.v1.CountTranslationKeysRequest;
import com.acme.localization.v1.CountTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.GetPurchaseOrderRatesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRatesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.LintPurchaseRequisitionsRequest;
import com.acme.procurement.v1.LintPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderBankSettlement;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseOrderLineTax;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierContractRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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

class PurchaseRequisitionValidationApplicationTest {
    @Test
    void previewPurchaseRequisitionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionValidationApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                AuditTrailAdminServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                TranslationKeyQueryServiceGrpc.newFutureStub(channel),
                PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewPurchaseRequisitionRequest.getDefaultInstance());
        var response = application.previewPurchaseRequisition(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseOrderCalculationService/QuotePurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent0, "purchase_order", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "lines", "tax", "rate"),
                ProbeSupport.value(sent0, "purchase_order", "lines", "tax", "rate"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseOrderCalculationService/EstimatePurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "card", "network"),
                ProbeSupport.value(sent1, "purchase_order", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "updated_at"),
                ProbeSupport.value(sent1, "purchase_order", "audit", "updated_at"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailAdminService/RepairAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/EstimatePurchaseOrder"), "purchase_order", "audit", "created_by"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/QuotePurchaseOrder"), "purchase_order", "audit", "updated_at"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/QuotePurchaseOrder"), "purchase_order", "card", "network"),
                ProbeSupport.value(sent2, "audit_trail", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address", "region"),
                ProbeSupport.value(sent2, "audit_trail", "billing_address", "region"));
        var sent3 = channel.request("acme.contracts.v1.RenewalValidationService/NormalizeRenewal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent3, "renewal", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailAdminService/RepairAuditTrail"), "audit_trail", "billing_address", "postal_code"),
                ProbeSupport.value(sent3, "renewal", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/EstimatePurchaseOrder"), "purchase_order", "billing_address", "recipient"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/EstimatePurchaseOrder"), "purchase_order", "metadata"),
                ProbeSupport.value(response, "purchase_requisition", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailAdminService/RepairAuditTrail"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition_id"),
                ProbeSupport.value(response, "purchase_requisition", "purchase_requisition_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "status"),
                ProbeSupport.value(response, "purchase_requisition", "status"));
    }
}
