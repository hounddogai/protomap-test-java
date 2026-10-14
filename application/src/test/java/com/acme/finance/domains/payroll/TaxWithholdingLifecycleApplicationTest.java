package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.ScheduleBudgetExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetExpiryResponse;
import com.acme.catalog.v1.Product;
import com.acme.catalog.v1.ProductCalculationServiceGrpc;
import com.acme.catalog.v1.RecalculateProductsRequest;
import com.acme.catalog.v1.RecalculateProductsResponse;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.EstimateCertificateRequest;
import com.acme.certificates.v1.EstimateCertificateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.forms.v1.SummarizeFormsRequest;
import com.acme.forms.v1.SummarizeFormsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.payroll.v1.ActivateTaxWithholdingRequest;
import com.acme.payroll.v1.ActivateTaxWithholdingResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionRouteAction;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExpireTaxWithholdingRequest;
import com.acme.payroll.v1.ExpireTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleRequest;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.ReinstateTaxWithholdingRequest;
import com.acme.payroll.v1.ReinstateTaxWithholdingResponse;
import com.acme.payroll.v1.RenewTaxWithholdingRequest;
import com.acme.payroll.v1.RenewTaxWithholdingResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.SuspendTaxWithholdingRequest;
import com.acme.payroll.v1.SuspendTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminateTaxWithholdingRequest;
import com.acme.payroll.v1.TerminateTaxWithholdingResponse;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.seating.v1.BrowseSeatMapsRequest;
import com.acme.seating.v1.BrowseSeatMapsResponse;
import com.acme.seating.v1.SeatMapQueryServiceGrpc;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class TaxWithholdingLifecycleApplicationTest {
    @Test
    void terminateTaxWithholdingPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new TaxWithholdingLifecycleApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                BudgetLifecycleServiceGrpc.newBlockingStub(channel),
                ProductCalculationServiceGrpc.newFutureStub(channel),
                CertificateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                ExpenseReportApprovalServiceGrpc.newBlockingStub(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                new TaxWithholdingLifecycleRequestMapperImpl(),
                DeductionLifecycleServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newFutureStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestLifecycleServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SeatMapQueryServiceGrpc.newBlockingStub(channel),
                SupplierContractWorkflowServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateTaxWithholdingRequest.getDefaultInstance());
        var response = application.terminateTaxWithholding(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "budget", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        var sent1 = channel.request("acme.documents.v1.DocumentService/ListDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "budget", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierContractWorkflowService/ListSupplierContractWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "budget", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "budget", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.forms.v1.FormQueryService/SummarizeForms");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent3, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "result", "details"),
                ProbeSupport.value(sent3, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLifecycleService/ScheduleBudgetExpiry"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentService/ListDocuments"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentService/ListDocuments"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractWorkflowService/ListSupplierContractWorkflows"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding_id"),
                ProbeSupport.value(response, "tax_withholding", "tax_withholding_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
