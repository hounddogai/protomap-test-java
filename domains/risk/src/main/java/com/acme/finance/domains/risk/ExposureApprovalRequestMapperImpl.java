package com.acme.finance.domains.risk;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.risk.v1.ApproveExposureRequest;
import com.acme.risk.v1.ApproveExposureResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CommentExposureApprovalRequest;
import com.acme.risk.v1.CommentExposureApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureLimit;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetExposureApprovalStatusRequest;
import com.acme.risk.v1.GetExposureApprovalStatusResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.RecallExposureRequest;
import com.acme.risk.v1.RecallExposureResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectExposureRequest;
import com.acme.risk.v1.RejectExposureResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.google.protobuf.Any;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ExposureApprovalRequestMapperImpl implements ExposureApprovalRequestMapper {
    @Override
    public PullUnderwritingsRequest submitExposureStage1(SubmitExposureRequest request, RetryLoanWorkflowResponse reply0) {
        return PullUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }
}
