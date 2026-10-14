package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.GetPurchaseOrderRatesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRatesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.google.protobuf.Any;
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

class PurchaseOrderCalculationApplicationTest {
    @Test
    void calculatePurchaseOrderPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseOrderCalculationApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newFutureStub(channel),
                repository,
                RiskRegisterServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(CalculatePurchaseOrderRequest.getDefaultInstance());
        var response = application.calculatePurchaseOrder(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent0, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "bank", "iban"),
                ProbeSupport.value(sent0, "purchase_requisition", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "postal_code"),
                ProbeSupport.value(sent0, "purchase_requisition", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "counterparty", "number"),
                ProbeSupport.value(sent0, "purchase_requisition", "counterparty", "number"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/ExplainPurchaseRequisitionCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "purchase_requisition", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "card", "network"),
                ProbeSupport.value(sent1, "purchase_requisition", "card", "network"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterService/UpdateRiskRegister");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "locality"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "locality"));
        var sent3 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/EstimatePurchaseRequisition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent3, "purchase_requisition", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "lines", "tax", "amount"),
                ProbeSupport.value(sent3, "purchase_requisition", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "counterparty", "number"),
                ProbeSupport.value(response, "purchase_order", "counterparty", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ExplainPurchaseRequisitionCalculation"), "purchase_requisition", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterService/UpdateRiskRegister"), "risk_register", "location", "lines"),
                ProbeSupport.value(response, "purchase_order", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order_id"),
                ProbeSupport.value(response, "purchase_order", "purchase_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}
