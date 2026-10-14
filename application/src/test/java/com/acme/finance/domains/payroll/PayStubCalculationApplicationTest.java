package com.acme.finance.domains.payroll;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetPayStubRatesRequest;
import com.acme.payroll.v1.GetPayStubRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunAttachment;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.procurement.v1.CreatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CreatePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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

class PayStubCalculationApplicationTest {
    @Test
    void calculatePayStubPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayStubCalculationApplication(
                ChannelRegistry.fixed(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CalculatePayStubRequest.getDefaultInstance());
        var response = application.calculatePayStub(request);
        var sent0 = channel.request("acme.payroll.v1.PayRunCalculationService/SimulatePayRun");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent0, "pay_run", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        var sent1 = channel.request("acme.payroll.v1.PayRunCalculationService/QuotePayRun");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "lines", "tax", "amount"),
                ProbeSupport.value(sent1, "pay_run", "lines", "tax", "amount"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseRequisitionService/CreatePurchaseRequisition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "file_name"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "file_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "content_type"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "uri"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "uri"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "size_bytes"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "size_bytes"));
        var sent3 = channel.request("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "checksum"),
                ProbeSupport.value(sent3, "pay_run", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/SimulatePayRun"), "pay_run", "audit", "etag"),
                ProbeSupport.value(response, "pay_stub", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/SimulatePayRun"), "pay_run", "audit", "created_by"),
                ProbeSupport.value(response, "pay_stub", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub_id"),
                ProbeSupport.value(response, "pay_stub", "pay_stub_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}
