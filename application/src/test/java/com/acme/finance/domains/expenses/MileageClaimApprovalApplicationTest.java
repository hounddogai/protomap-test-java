package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.banking.v1.ListPendingStandingOrdersRequest;
import com.acme.banking.v1.ListPendingStandingOrdersResponse;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.ListPendingBudgetLinesRequest;
import com.acme.budgeting.v1.ListPendingBudgetLinesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.expenses.v1.ApproveMileageClaimRequest;
import com.acme.expenses.v1.ApproveMileageClaimResponse;
import com.acme.expenses.v1.CommentMileageClaimApprovalRequest;
import com.acme.expenses.v1.CommentMileageClaimApprovalResponse;
import com.acme.expenses.v1.EscalateMileageClaimRequest;
import com.acme.expenses.v1.EscalateMileageClaimResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpireMileageClaimRequest;
import com.acme.expenses.v1.ExpireMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusRequest;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusResponse;
import com.acme.expenses.v1.ListPendingReceiptsRequest;
import com.acme.expenses.v1.ListPendingReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.RejectMileageClaimRequest;
import com.acme.expenses.v1.RejectMileageClaimResponse;
import com.acme.expenses.v1.RenewMileageClaimRequest;
import com.acme.expenses.v1.RenewMileageClaimResponse;
import com.acme.expenses.v1.SubmitMileageClaimRequest;
import com.acme.expenses.v1.SubmitMileageClaimResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.licensing.v1.CreateSoftwareLicenseRequest;
import com.acme.licensing.v1.CreateSoftwareLicenseResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseCardFunding;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.webhooks.v1.ListWebhookEndpointsRequest;
import com.acme.webhooks.v1.ListWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class MileageClaimApprovalApplicationTest {
    @Test
    void submitMileageClaimPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimApprovalApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                MileageClaimLifecycleServiceGrpc.newBlockingStub(channel),
                ReceiptApprovalServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                WebhookEndpointServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitMileageClaimRequest.getDefaultInstance());
        var response = application.submitMileageClaim(request);
        var sent0 = channel.request("acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent0, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "details", "destination", "country_code"));
        var sent1 = channel.request("acme.expenses.v1.ReceiptApprovalService/ListPendingReceipts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailSyncService/ReconcileAuditTrails");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "audit_trail", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "card", "last4"),
                ProbeSupport.value(sent2, "audit_trail", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "updated_at"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "updated_at"));
        var sent3 = channel.request("acme.compliance.v1.AttestationWorkflowService/SignalAttestationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord"), "consent_record", "subject", "display_name"),
                ProbeSupport.value(sent3, "attestation", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptApprovalService/ListPendingReceipts"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ReconcileAuditTrails"), "audit_trail", "total"),
                ProbeSupport.value(response, "mileage_claim", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim_id"),
                ProbeSupport.value(response, "mileage_claim", "mileage_claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address"),
                ProbeSupport.value(response, "mileage_claim", "billing_address"));
    }
}
