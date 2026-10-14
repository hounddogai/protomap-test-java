package com.acme.finance.domains.procurement;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ClonePurchaseRequisitionRequest;
import com.acme.procurement.v1.ClonePurchaseRequisitionResponse;
import com.acme.procurement.v1.CreatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CreatePurchaseRequisitionResponse;
import com.acme.procurement.v1.DeletePurchaseRequisitionRequest;
import com.acme.procurement.v1.DeletePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.acme.procurement.v1.WatchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.WatchPurchaseRequisitionsResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
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

class PurchaseRequisitionApplicationTest {
    @Test
    void updatePurchaseRequisitionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                new PurchaseRequisitionRequestMapperImpl(),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(UpdatePurchaseRequisitionRequest.getDefaultInstance());
        var response = application.updatePurchaseRequisition(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "card", "holder_name"),
                ProbeSupport.value(sent0, "purchase_order", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "created_by"),
                ProbeSupport.value(sent0, "purchase_order", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "card", "last4"),
                ProbeSupport.value(sent0, "purchase_order", "card", "last4"));
        var sent1 = channel.request("acme.procurement.v1.GoodsReceiptCalculationService/QuoteGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "goods_receipt", "counterparty", "display_name"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseRequisitionValidationService/NormalizePurchaseRequisition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "purchase_order", "card", "token"),
                ProbeSupport.value(sent2, "purchase_requisition", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address", "region"),
                ProbeSupport.value(sent2, "purchase_requisition", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/QuoteGoodsReceipt"), "breakdown", "total"),
                ProbeSupport.value(response, "purchase_requisition", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/NormalizePurchaseRequisition"), "purchase_requisition", "purchase_requisition_id"),
                ProbeSupport.value(response, "purchase_requisition", "purchase_requisition_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "counterparty"),
                ProbeSupport.value(response, "purchase_requisition", "counterparty"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address"));
    }

    @Test
    void watchPurchaseRequisitionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                new PurchaseRequisitionRequestMapperImpl(),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(WatchPurchaseRequisitionsRequest.getDefaultInstance());
        var response = application.watchPurchaseRequisitions(request);
        var sent0 = channel.request("acme.compliance.v1.AttestationValidationService/ValidateAttestation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "attestation", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationValidationService/ValidateAttestation"), "attestation", "billing_address", "recipient"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationValidationService/ValidateAttestation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address", "region"));
        var endpoint = new PurchaseRequisitionEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchPurchaseRequisitionsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchPurchaseRequisitionsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchPurchaseRequisitions(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreatePurchaseRequisitionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                new PurchaseRequisitionRequestMapperImpl(),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(BatchCreatePurchaseRequisitionsRequest.getDefaultInstance());
        var response = application.batchCreatePurchaseRequisitions(request);
        var sent0 = channel.request("acme.procurement.v1.RfqValidationService/ExplainRfqDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "rfq", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "name"),
                ProbeSupport.value(sent0, "rfq", "requester", "name"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/ExplainPurchaseRequisitionCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/ExplainRfqDecision"), "rfq", "requester", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "purchase_requisition", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "counterparty", "number"),
                ProbeSupport.value(sent1, "purchase_requisition", "counterparty", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/ExplainRfqDecision"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ExplainPurchaseRequisitionCalculation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        var endpoint = new PurchaseRequisitionEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreatePurchaseRequisitionsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreatePurchaseRequisitionsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreatePurchaseRequisitions(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
