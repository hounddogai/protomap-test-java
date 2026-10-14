package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AdvanceFixedAssetWorkflowRequest;
import com.acme.assets.v1.AdvanceFixedAssetWorkflowResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.CountVarianceTotalsRequest;
import com.acme.budgeting.v1.CountVarianceTotalsResponse;
import com.acme.budgeting.v1.DeleteVarianceReportRequest;
import com.acme.budgeting.v1.DeleteVarianceReportResponse;
import com.acme.budgeting.v1.FetchVarianceReportRequest;
import com.acme.budgeting.v1.FetchVarianceReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.GenerateVarianceReportRequest;
import com.acme.budgeting.v1.GenerateVarianceReportResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.ScheduleVarianceReportRequest;
import com.acme.budgeting.v1.ScheduleVarianceReportResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.StreamVarianceMetricsRequest;
import com.acme.budgeting.v1.StreamVarianceMetricsResponse;
import com.acme.budgeting.v1.UpdateBudgetRequest;
import com.acme.budgeting.v1.UpdateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceSla;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.licensing.v1.CompareSoftwareLicenseOptionsRequest;
import com.acme.licensing.v1.CompareSoftwareLicenseOptionsResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseBankFunding;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierContractRef;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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

class VarianceReportingApplicationTest {
    @Test
    void streamVarianceMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new VarianceReportingApplication(
                FixedAssetWorkflowServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newFutureStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                VarianceSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationWorkflowServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newBlockingStub(channel),
                ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                new VarianceReportingRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamVarianceMetricsRequest.getDefaultInstance());
        var response = application.streamVarianceMetrics(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "variance", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "variance", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "variance", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "variance", "requester", "preferred_channel"));
        var sent1 = channel.request("acme.budgeting.v1.VarianceImportService/ListVarianceImports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "statuses"),
                ProbeSupport.value(sent1, "filter", "statuses"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailHistoryService/RecordAuditTrailVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "audit", "created_at"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "created_at"));
        var sent3 = channel.request("acme.assets.v1.FixedAssetWorkflowService/AdvanceFixedAssetWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "assignee", "display_name"),
                ProbeSupport.value(sent3, "fixed_asset", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RecordAuditTrailVersion"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent3, "fixed_asset", "address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "audit", "updated_by"),
                ProbeSupport.value(sent3, "fixed_asset", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "assignee", "user_id"),
                ProbeSupport.value(response, "variance", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ResolveVarianceConflict"), "variance", "status"),
                ProbeSupport.value(response, "variance", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceImportService/ListVarianceImports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RecordAuditTrailVersion"), "audit_trail", "audit", "created_at"),
                ProbeSupport.value(response, "variance", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RecordAuditTrailVersion"), "audit_trail", "due_at"),
                ProbeSupport.value(response, "variance", "sla", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetWorkflowService/AdvanceFixedAssetWorkflow"), "fixed_asset", "owner", "email"),
                ProbeSupport.value(response, "variance", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "variance", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(response, "variance", "requester", "preferred_channel"));
        var endpoint = new VarianceReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamVarianceMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamVarianceMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamVarianceMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
