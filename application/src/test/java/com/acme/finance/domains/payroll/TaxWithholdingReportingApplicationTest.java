package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionBlockAction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.DeleteTaxWithholdingReportRequest;
import com.acme.payroll.v1.DeleteTaxWithholdingReportResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.FetchTaxWithholdingReportRequest;
import com.acme.payroll.v1.FetchTaxWithholdingReportResponse;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.ListTaxWithholdingReportsRequest;
import com.acme.payroll.v1.ListTaxWithholdingReportsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingRouteAction;
import com.acme.personalization.v1.ProfileServiceGrpc;
import com.acme.personalization.v1.SearchProfilesRequest;
import com.acme.personalization.v1.SearchProfilesResponse;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.SubmitPurchaseRequisitionRequest;
import com.acme.procurement.v1.SubmitPurchaseRequisitionResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class TaxWithholdingReportingApplicationTest {
    @Test
    void shareTaxWithholdingReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new TaxWithholdingReportingApplicationEvents(TaxWithholdingCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof TaxWithholdingReportingApplication.ScheduleTaxWithholdingReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new TaxWithholdingReportingApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                new TaxWithholdingReportingRequestMapperImpl(),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                DeductionValidationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                ProfileServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionWorkflowServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ShareTaxWithholdingReportRequest.getDefaultInstance());
        var response = application.shareTaxWithholdingReport(request);
        var sent0 = channel.request("acme.payroll.v1.DeductionValidationService/NormalizeDeduction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.payroll.v1.PayRunService/ListPayRuns");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/NormalizeDeduction"), "deduction", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/NormalizeDeduction"), "deduction", "audit", "created_at"),
                ProbeSupport.value(sent2, "tax_withholding", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/NormalizeDeduction"), "deduction", "route", "target"),
                ProbeSupport.value(sent2, "tax_withholding", "route", "target"));
        var sent3 = channel.request("acme.insurance.v1.UnderwritingReportingService/ShareUnderwritingReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings"), "tax_withholding", "owner", "email"),
                ProbeSupport.value(sent3, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent3, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(sent3, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "channel"),
                ProbeSupport.value(sent3, "details", "channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/NormalizeDeduction"), "deduction", "audit", "created_at"),
                ProbeSupport.value(response, "tax_withholding", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunService/ListPayRuns"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunService/ListPayRuns"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings"), "tax_withholding", "audit", "etag"),
                ProbeSupport.value(response, "tax_withholding", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings"), "tax_withholding", "name"),
                ProbeSupport.value(response, "tax_withholding", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding_id"),
                ProbeSupport.value(response, "tax_withholding", "tax_withholding_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(response, "tax_withholding", "owner", "email"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new TaxWithholdingReportingApplicationEvents(TaxWithholdingCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimateTaxWithholdingResponse>();
        listener.on(new TaxWithholdingReportingApplication.ScheduleTaxWithholdingReportStage1Requested(ProbeSupport.sample(EstimateTaxWithholdingRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
