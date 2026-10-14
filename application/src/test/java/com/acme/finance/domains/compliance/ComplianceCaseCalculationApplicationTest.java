package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseRatesRequest;
import com.acme.compliance.v1.GetComplianceCaseRatesResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ResolveUsersRequest;
import com.acme.identity.v1.ResolveUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.ExportLoansRequest;
import com.acme.lending.v1.ExportLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lineage.v1.LineageNodeQueryServiceGrpc;
import com.acme.lineage.v1.SuggestLineageNodesRequest;
import com.acme.lineage.v1.SuggestLineageNodesResponse;
import com.acme.media.v1.MediaAsset;
import com.acme.media.v1.MediaAssetCalculationServiceGrpc;
import com.acme.media.v1.RecalculateMediaAssetsRequest;
import com.acme.media.v1.RecalculateMediaAssetsResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.NormalizeMetricSeriesRequest;
import com.acme.metrics.v1.NormalizeMetricSeriesResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
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

class ComplianceCaseCalculationApplicationTest {
    @Test
    void quoteComplianceCasePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseCalculationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newFutureStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                LineageNodeQueryServiceGrpc.newBlockingStub(channel),
                MediaAssetCalculationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(QuoteComplianceCaseRequest.getDefaultInstance());
        var response = application.quoteComplianceCase(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationService/ListNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.notifications.v1.NotificationService/CloneNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "assignee", "user_id"),
                ProbeSupport.value(sent1, "notification", "assignee", "user_id"));
        var sent2 = channel.request("acme.lineage.v1.LineageNodeQueryService/SuggestLineageNodes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "history", "changed_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/ListNotifications"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/ListNotifications"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "priority"),
                ProbeSupport.value(response, "compliance_case", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case_id"),
                ProbeSupport.value(response, "compliance_case", "compliance_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "custom_fields"),
                ProbeSupport.value(response, "compliance_case", "custom_fields"));
    }
}
