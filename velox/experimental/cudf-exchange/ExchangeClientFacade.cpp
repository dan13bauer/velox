#include "velox/experimental/cudf-exchange/ExchangeClientFacade.h"
#include "velox/experimental/cudf-exchange/NetUtil.h"

namespace facebook::velox::cudf_exchange {

ExchangeClientFacade::ExchangeClientFacade(
    std::shared_ptr<CudfExchangeClient> cudfExchangeClient,
    std::shared_ptr<ExchangeClient> httpExchangeClient)
    : cudfExchangeClient_{cudfExchangeClient},
      httpExchangeClient_{httpExchangeClient},
      kCoordinatorUri_{Communicator::getInstance()->getCoordinatorUrl()} {}

void ExchangeClientFacade::activateCudfExchangeClient() {
  VELOX_CHECK(!usesHttp_, "Can't switch from Cudf to Http while operator is active.");
  if (usesCudf_) {
    // already activated.
    return;
  }
  addRemoteTaskId_ =
      [c = cudfExchangeClient_](const std::string& remoteTaskId) {
        c->addRemoteTaskId(remoteTaskId);
      };
  noMoreRemoteTasks_ = [c = cudfExchangeClient_]() { c->noMoreRemoteTasks(); };
  next_ = [c = cudfExchangeClient_](
              int consumerId,
              uint32_t maxBytes,
              bool* atEnd,
              facebook::velox::ContinueFuture* future) -> ResultVariant {
    return c->next(consumerId, atEnd, future);
  };
  stats_ = [c = cudfExchangeClient_]() { return c->stats(); };
  close_ = [c = cudfExchangeClient_]() { c->close(); };
  usesCudf_ = true;
}

void ExchangeClientFacade::activateHttpExchangeClient() {
  VELOX_CHECK(!usesCudf_, "Can't switch from Http to Cudf while operator is active.");
  if (usesHttp_) {
    // already activated.
    return;
  }
  addRemoteTaskId_ =
      [c = httpExchangeClient_](const std::string& remoteTaskId) {
        c->addRemoteTaskId(remoteTaskId);
      };
  noMoreRemoteTasks_ = [c = httpExchangeClient_]() { c->noMoreRemoteTasks(); };
  next_ = [c = httpExchangeClient_](
              int consumerId,
              uint32_t maxBytes,
              bool* atEnd,
              facebook::velox::ContinueFuture* future) -> ResultVariant {
    return c->next(consumerId, maxBytes, atEnd, future);
  };
  stats_ = [c = httpExchangeClient_]() { return c->stats(); };
  close_ = [c = httpExchangeClient_]() { c->close(); };
  usesHttp_ = true;
}

void ExchangeClientFacade::addRemoteTaskId(
    const std::string& remoteTaskId) {
  // dissect the remote task id.
  folly::Uri uri(remoteTaskId);
  const std::string host = uri.host();
  int port = uri.port();
  if (isSameHost(host, kCoordinatorUri_.host()) &&
      (port == kCoordinatorUri_.port())) {
    VLOG(3) << "Activating HTTP exchange client for remote task id: " << remoteTaskId;
    activateHttpExchangeClient();
  } else {
    VLOG(3) << "Activating Cudf exchange client for remote task id: " << remoteTaskId;
    activateCudfExchangeClient();
  }
  addRemoteTaskId_(remoteTaskId);
}

void ExchangeClientFacade::noMoreRemoteTasks() {
  VELOX_CHECK(noMoreRemoteTasks_, "noMoreRemoteTasks called but no client set!");
  noMoreRemoteTasks_();
}

// Depending on the underlying client, a different return type is used.
ResultVariant ExchangeClientFacade::next(
    int consumerId,
    uint32_t maxBytes,
    bool* atEnd,
    facebook::velox::ContinueFuture* future) {
  if (!usesCudf_ && !usesHttp_) {
    // not using any client yet - return empty result and no valid future.
    ResultVariant emptyResult;
    return emptyResult;
  }
  return next_(consumerId, maxBytes, atEnd, future);
}

void ExchangeClientFacade::close() {
  VELOX_CHECK(close_, "close called but no client set!");
  close_();
}

folly::F14FastMap<std::string, facebook::velox::RuntimeMetric>
ExchangeClientFacade::stats() {
  VELOX_CHECK(stats_, "stats called but no client set!");
  return stats_();
}


}