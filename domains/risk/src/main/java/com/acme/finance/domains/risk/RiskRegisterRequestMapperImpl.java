package com.acme.finance.domains.risk;

import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CloneRiskRegisterRequest;
import com.acme.risk.v1.CloneRiskRegisterResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.CreateRiskRegisterRequest;
import com.acme.risk.v1.CreateRiskRegisterResponse;
import com.acme.risk.v1.DeleteRiskRegisterRequest;
import com.acme.risk.v1.DeleteRiskRegisterResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetRiskRegisterRequest;
import com.acme.risk.v1.GetRiskRegisterResponse;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.SearchRiskRegistersRequest;
import com.acme.risk.v1.SearchRiskRegistersResponse;
import com.acme.risk.v1.SummarizeRiskScenariosRequest;
import com.acme.risk.v1.SummarizeRiskScenariosResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class RiskRegisterRequestMapperImpl implements RiskRegisterRequestMapper {
    @Override
    public LintVenuesRequest cloneRiskRegisterStage1(CloneRiskRegisterRequest request, ValidateRiskScenarioResponse reply0) {
        return LintVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintVenuesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getRiskScenario().getRequester().getName())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRiskRegister().getLocation().getPostalCode())
                                .setRegion(request.getRiskRegister().getLocation().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }
}
