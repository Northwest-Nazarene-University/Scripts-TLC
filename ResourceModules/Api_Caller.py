## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, time, functools, threading, random, requests
from datetime import datetime
from typing import Callable, Tuple, Type, Optional, Dict, Any, List

try: ## If the module is run directly
    from Local_Setup import LocalSetup
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Provides the ApiCaller class for making HTTP API calls with retry, rate-limiting,
Canvas-specific 429 handling, and support for both Canvas and non-Canvas endpoints.
"""

externalRequirements = r"""
To be located within the ResourceModules folder alongside TLC_Common.py.
Requires Common_Configs for canvasAccessToken.
"""

## Add the config path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Configs"))

from Common_Configs import canvasAccessToken

## -------------------------
## Canvas Detection
## -------------------------

## Marker used to detect Canvas API calls; checked against the URL host segment
canvasUrlMarker = ".instructure.com"

## -------------------------
## Rate-Limit Configuration
## -------------------------

## Default seconds to wait when Canvas returns HTTP 429 and no Retry-After header exists
baseRateLimitWaitSeconds: float = 2.0

## Maximum jitter added to rate-limit waits (Uniform(0, Max))
## Average jitter = Max / 2
rateLimitJitterMaxSeconds: float = 2.0

## Backoff applied ONLY to repeated 429 waits when Retry-After is missing
rateLimitBackoffMultiplier: float = 1.5
maxThrottleRetries: int = 10

## Pre-emptive pause when remaining quota is low
rateLimitPauseThreshold: float = 50.0
basePreemptivePauseSeconds: float = 0.5
preemptivePauseJitterMaxSeconds: float = 0.25

## Hard timeout for requests to prevent hanging forever
requestTimeoutSeconds: float = 600.0

## Shared rate-limit state across threads in this process
_rateLimitRemaining: Optional[float] = None
_rateLimitLock = threading.Lock()

## -------------------------
## Global Concurrency Gate (Canvas-only)
## -------------------------
## Canvas uses a token-bucket rate limiter shared across ALL requests for the same
## API token.  When dozens of threads fire simultaneously, each one independently
## hits 429 and independently backs off — creating a thundering-herd effect that
## wastes time and generates massive log spam.
##
## Solution:
##   1. _canvasApiSemaphore — limits how many threads can be inside the HTTP dispatch
##      at the same time.  This prevents overwhelming the bucket in the first place.
##   2. _canvasApiGate — a threading.Event that is normally "set" (open).  When ANY
##      thread receives a 429, it "clears" the gate (blocking) for the cooldown
##      duration.  ALL other threads block on gate.wait() before they can dispatch,
##      so only ONE coordinated pause happens instead of N independent ones.

## Maximum concurrent Canvas API requests.  Canvas refills ~10 tokens/second for
## most institutions.  Keeping this at the refill rate prevents steady-state 429s.
_canvasMaxConcurrentRequests: int = 10
_canvasApiSemaphore = threading.Semaphore(_canvasMaxConcurrentRequests)

## Global cooldown gate — cleared (blocking) during a 429 cooldown, set (open) normally
_canvasApiGate = threading.Event()
_canvasApiGate.set()  ## Start open

_gateLock = threading.Lock()
_gateReopenTime: float = 0.0  ## time.monotonic() when the gate should reopen


## -------------------------
## Custom Exceptions
## -------------------------

class RateLimitExceeded(Exception):
    """Raised when an API returns HTTP 429 (rate limit exceeded)."""
    def __init__(self, retryAfter: Optional[float] = None, message: str = "Rate limit exceeded"):
        self.retryAfter = retryAfter
        super().__init__(message)


## -------------------------
## Retry Decorator (Separate 429 Lane)
## -------------------------

def retry(
    max_attempts: int = 5,
    delay: float = 5.0,
    backoff: float = 1.5,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_throttle_retries: int = maxThrottleRetries,
):
    """
    Retry decorator with a separate retry lane for HTTP 429 (RateLimitExceeded).

    Supports both standalone functions where the first argument is a LocalSetup instance,
    and class methods where the first argument is an object with a .localSetup attribute
    (e.g., an ApiCaller instance).

    Rate-limit retries (RateLimitExceeded) are handled separately and do not count
    against max_attempts.

    CHANGED from original: The per-thread sleep on 429 is removed.  Instead,
    _triggerGlobalCooldown() blocks ALL threads.  The retry loop here just
    catches the exception and loops back to the top of makeApiCall, where the
    gate.wait() will block until the global cooldown expires.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            firstArg = args[0]
            ## Support LocalSetup as first arg or objects with .localSetup (e.g. ApiCaller)
            localSetup = getattr(firstArg, 'localSetup', firstArg)

            attempts = 0
            throttleRetries = 0

            currentDelaySeconds = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)

                except RateLimitExceeded as rateLimitError:
                    throttleRetries += 1

                    if getattr(localSetup, "logger", None):
                        localSetup.logger.warning(
                            f"Rate limit hit for {func.__name__} "
                            f"(throttle retry {throttleRetries}/{max_throttle_retries}). "
                            f"Global cooldown triggered — waiting for gate to reopen..."
                        )

                    if throttleRetries >= max_throttle_retries:
                        if getattr(localSetup, "logger", None):
                            localSetup.logger.error(
                                f"{func.__name__} exceeded max throttle retries ({max_throttle_retries})."
                            )
                        raise

                    ## No per-thread sleep here.  The global cooldown (_triggerGlobalCooldown)
                    ## was already initiated by makeApiCall before raising RateLimitExceeded.
                    ## When we loop back, makeApiCall will gate.wait() before dispatching,
                    ## which blocks until the coordinated cooldown expires.

                except exceptions as error:
                    attempts += 1

                    if getattr(localSetup, "logger", None):
                        localSetup.logger.warning(
                            f"Attempt {attempts} failed for {func.__name__}: {error}. "
                            f"Retrying in {currentDelaySeconds:.1f} seconds..."
                        )

                    if attempts == max_attempts:
                        if getattr(localSetup, "logger", None):
                            localSetup.logger.error(f"{func.__name__} failed after {attempts} attempts.")
                        raise

                    time.sleep(currentDelaySeconds)
                    currentDelaySeconds *= backoff
        return wrapper
    return decorator


## -------------------------
## Email Helper (Best Effort)
## -------------------------

def _sendTimeoutEmail(localSetup, apiUrl: str, timeoutSeconds: float, error: Exception) -> None:
    """
    Best-effort timeout notification.
    Tries common LocalSetup email methods; falls back to logging.
    """
    subject = f"API Timeout ({timeoutSeconds:.0f}s)"
    body = (
        f"An API request timed out.\n\n"
        f"Script Context: {localSetup.__scriptName}\n"
        f"URL: {apiUrl}\n"
        f"Timeout: {timeoutSeconds:.0f}s\n"
        f"Error: {error}\n"
        f"Timestamp: {datetime.now().isoformat()}\n"
    )

    ## Try likely method names without assuming the LocalSetup implementation
    for methodName in ["SendErrorEmail", "sendErrorEmail", "SendEmail", "sendEmail"]:
        sendMethod = getattr(localSetup, methodName, None)
        if callable(sendMethod):
            try:
                ## Support both (subject, body) and keyword forms
                try:
                    sendMethod(subject, body)
                except TypeError:
                    sendMethod(Subject=subject, Body=body)
                return
            except Exception as emailError:
                if getattr(localSetup, "logger", None):
                    localSetup.logger.error(f"Failed sending timeout email via {methodName}: {emailError}")

    if getattr(localSetup, "logger", None):
        localSetup.logger.error(f"No email method found on LocalSetup. Timeout email not sent.\n{subject}\n{body}")


## -------------------------
## Rate-Limit Header Helpers (Canvas-only)
## -------------------------

def _updateRateLimitRemainingFromResponse(responseObject) -> None:
    global _rateLimitRemaining

    rawRemaining = responseObject.headers.get("X-Rate-Limit-Remaining")
    if rawRemaining is None:
        return

    try:
        remainingValue = float(rawRemaining)
    except (ValueError, TypeError):
        return

    with _rateLimitLock:
        _rateLimitRemaining = remainingValue


def _preemptiveRateLimitPauseIfNeeded(localSetup, apiUrl: str) -> None:
    """
    If the most recently observed X-Rate-Limit-Remaining is below the threshold,
    pause briefly before making the next request.  This is a best-effort hint;
    the global gate is the real enforcer.
    """
    with _rateLimitLock:
        currentRemaining = _rateLimitRemaining

    if currentRemaining is None:
        return

    if currentRemaining <= rateLimitPauseThreshold:
        jitterSeconds = random.uniform(0.0, preemptivePauseJitterMaxSeconds)
        pauseSeconds = basePreemptivePauseSeconds + jitterSeconds

        if getattr(localSetup, "logger", None):
            localSetup.logger.info(
                f"Rate-limit remaining low ({currentRemaining:.1f} <= {rateLimitPauseThreshold:.1f}). "
                f"Preemptive pause {pauseSeconds:.2f}s before {apiUrl}."
            )

        time.sleep(pauseSeconds)


## -------------------------
## Global Cooldown Functions
## -------------------------

def _triggerGlobalCooldown(waitSeconds: float, localSetup=None) -> None:
    """
    Called when ANY thread receives a 429.  Closes the gate so ALL threads
    wait until the cooldown expires, then reopens it.

    Only the first thread to trigger the cooldown actually sleeps and reopens
    the gate.  Subsequent threads that call this while the gate is already
    closed will see that _gateReopenTime is already far enough in the future
    and return immediately (they will block on _canvasApiGate.wait() instead).
    """
    global _gateReopenTime

    reopenAt = time.monotonic() + waitSeconds

    with _gateLock:
        ## Only extend the cooldown — never shorten it
        if reopenAt <= _gateReopenTime:
            return  ## Another thread already set a longer (or equal) cooldown
        _gateReopenTime = reopenAt
        _canvasApiGate.clear()  ## Block all threads

    if localSetup and getattr(localSetup, "logger", None):
        localSetup.logger.warning(
            f"Global rate-limit cooldown: blocking all Canvas API threads for {waitSeconds:.1f}s"
        )

    ## This thread is responsible for reopening the gate
    time.sleep(waitSeconds)

    with _gateLock:
        ## Only reopen if no other thread extended the cooldown further
        if time.monotonic() >= _gateReopenTime:
            _canvasApiGate.set()
            if localSetup and getattr(localSetup, "logger", None):
                localSetup.logger.info("Global rate-limit cooldown expired. Resuming API calls.")


## -------------------------
## ApiCaller Class
## -------------------------

class ApiCaller:
    """
    Manages API calls with retry logic, rate-limit handling, and Canvas-specific behaviors.

    Canvas calls are detected by the presence of 'instructure.com' in the URL and receive:
    - A default Authorization header using canvasAccessToken
    - X-Rate-Limit-Remaining tracking and preemptive pauses
    - Global concurrency semaphore limiting concurrent Canvas requests
    - Global cooldown gate — when ANY thread gets 429, ALL threads pause together
    - HTTP 429 -> RateLimitExceeded (separate retry lane, does not consume max_attempts)
    - Canvas-specific 409 Conflict handling for report generation

    Non-Canvas calls receive generic behavior: no rate-limit tracking, no default auth header.
    Both call types share the same session for connection pooling and use a 600s hard timeout.
    """

    def __init__(self, localSetup):
        self.localSetup = localSetup

    def _isCanvasUrl(self, apiUrl: str) -> bool:
        """Returns True if the given URL is a Canvas API call."""
        return canvasUrlMarker in apiUrl

    @retry(max_attempts=5, delay=5, backoff=2.0)
    def makeApiCall(
        self,
        p1_apiUrl,
        p1_header=None,
        p1_payload=None,
        p1_files=None,
        p1_apiCallType="get",
        firstPageOnly=False,
    ) -> Tuple[requests.Response, List[requests.Response]]:
        """
        Makes an API call using localSetup.canvasSession and a 600s timeout.

        Canvas calls (URLs containing 'instructure.com'):
        - Default Authorization header using canvasAccessToken
        - Global concurrency gate (semaphore) limiting simultaneous requests
        - Global cooldown gate that blocks ALL threads when any thread gets 429
        - Preemptive pause when remaining quota is low (X-Rate-Limit-Remaining)
        - HTTP 429 raises RateLimitExceeded (handled separately by retry decorator)
        - Canvas-specific 409 Conflict handling

        Non-Canvas calls:
        - No default auth header (caller must provide)
        - No rate-limit quota tracking or preemptive pauses
        - HTTP 429 still raises RateLimitExceeded for retry

        Status validation (both):
        - Any 2xx is success
        - 400 is allowed (unchanged behavior)
        - DELETE failures log a warning and return None instead of raising
        """
        isCanvas = self._isCanvasUrl(p1_apiUrl)

        ## Defaults
        if p1_header is None:
            p1_header = {"Authorization": f"Bearer {canvasAccessToken}"} if isCanvas else {}
        if p1_payload is None:
            p1_payload = {}
        if p1_files is None:
            p1_files = {}

        session = self.localSetup.canvasSession

        ## Canvas-only: wait for global cooldown gate + preemptive pause + acquire semaphore
        if isCanvas:
            ## Block if a global cooldown is active (another thread got 429)
            _canvasApiGate.wait()

            ## Best-effort preemptive pause based on X-Rate-Limit-Remaining header
            _preemptiveRateLimitPauseIfNeeded(self.localSetup, p1_apiUrl)

            ## Limit concurrent Canvas requests to prevent overwhelming the bucket
            _canvasApiSemaphore.acquire()

        ## Dispatch
        try:
            if p1_apiCallType.lower() == "get":
                ## Canvas paginates with per_page; non-Canvas may not support this param
                if isCanvas:
                    p1_payload.setdefault("per_page", 100)
                responseObject = session.get(
                    url=p1_apiUrl,
                    headers=p1_header,
                    params=p1_payload,
                    timeout=requestTimeoutSeconds,
                )

            elif p1_apiCallType.lower() == "post":
                if p1_payload and p1_files:
                    responseObject = session.post(
                        url=p1_apiUrl,
                        headers=p1_header,
                        json=p1_payload,
                        files=p1_files,
                        timeout=requestTimeoutSeconds,
                    )
                elif p1_payload:
                    responseObject = session.post(
                        url=p1_apiUrl,
                        headers=p1_header,
                        params=p1_payload,
                        timeout=requestTimeoutSeconds,
                    )
                else:
                    responseObject = session.post(
                        url=p1_apiUrl,
                        headers=p1_header,
                        timeout=requestTimeoutSeconds,
                    )

            elif p1_apiCallType.lower() == "put":
                if p1_payload:
                    responseObject = session.put(
                        url=p1_apiUrl,
                        headers=p1_header,
                        json=p1_payload,
                        timeout=requestTimeoutSeconds,
                    )
                else:
                    responseObject = session.put(
                        url=p1_apiUrl,
                        headers=p1_header,
                        timeout=requestTimeoutSeconds,
                    )

            elif p1_apiCallType.lower() == "delete":
                if p1_payload:
                    responseObject = session.delete(
                        url=p1_apiUrl,
                        headers=p1_header,
                        params=p1_payload,
                        timeout=requestTimeoutSeconds,
                    )
                else:
                    responseObject = session.delete(
                        url=p1_apiUrl,
                        headers=p1_header,
                        timeout=requestTimeoutSeconds,
                    )

            else:
                raise ValueError(f"Unsupported API call type: {p1_apiCallType}")

        except requests.exceptions.Timeout as timeoutError:
            ## Log, send best-effort email, then raise so @retry can retry
            if getattr(self.localSetup, "logger", None):
                self.localSetup.logger.error(
                    f"Timeout after {requestTimeoutSeconds:.0f}s calling {p1_apiUrl}: {timeoutError}"
                )
            _sendTimeoutEmail(self.localSetup, p1_apiUrl, requestTimeoutSeconds, timeoutError)
            raise

        finally:
            ## Always release the semaphore so other threads can proceed
            if isCanvas:
                _canvasApiSemaphore.release()

        ## Canvas-only: update shared rate-limit remaining tracker from response headers
        if isCanvas:
            _updateRateLimitRemainingFromResponse(responseObject)

        ## -------------------------
        ## Handle 429 -> trigger global cooldown, then raise for retry
        ## -------------------------
        if responseObject.status_code == 429:
            retryAfterSeconds: Optional[float] = None

            rawRetryAfter = responseObject.headers.get("Retry-After")
            if rawRetryAfter:
                try:
                    retryAfterSeconds = float(rawRetryAfter)
                except (ValueError, TypeError):
                    retryAfterSeconds = None

            ## Determine how long to block ALL threads
            cooldownSeconds = retryAfterSeconds if retryAfterSeconds else baseRateLimitWaitSeconds

            try:
                responseObject.close()
            except Exception:
                pass

            ## Block ALL threads, not just this one
            _triggerGlobalCooldown(cooldownSeconds, self.localSetup)

            raise RateLimitExceeded(
                retryAfter=retryAfterSeconds,
                message=f"API rate limit exceeded for {p1_apiUrl}.",
            )

        ## -------------------------
        ## Validate response codes
        ## -------------------------

        statusCode = responseObject.status_code

        ## Keep historical behavior: 400 is allowed
        isAllowed400 = (statusCode == 400)

        ## Standard success is any 2xx
        isSuccess2xx = (200 <= statusCode < 300)

        if not statusCode or (not isSuccess2xx and not isAllowed400):
            if statusCode:
                ## Canvas-only: 409 Conflict handling for report generation (PUT/POST/PATCH)
                if isCanvas and statusCode == 409 and p1_apiCallType.lower() in ["put", "patch", "post"]:
                    if getattr(self.localSetup, "logger", None):
                        self.localSetup.logger.warning(
                            f"Received 409 Conflict for {p1_apiCallType.upper()} {p1_apiUrl}. "
                            f"Checking for active existing item..."
                        )

                    ## Retrieve current index
                    indexResponse, _ = self.makeApiCall(
                        p1_apiUrl=p1_apiUrl,
                        p1_header=p1_header,
                        p1_apiCallType="get",
                        firstPageOnly=True,
                    )

                    indexData = indexResponse.json() if hasattr(indexResponse, "json") else []

                    requestedParams = {
                        ## Canvas encodes report parameters as "parameters[<key>]" in the payload;
                        ## strip the outer wrapper to get the plain key for matching against stored report parameters.
                        key[len("parameters["):-1]: value
                        for key, value in p1_payload.items()
                        if key.startswith("parameters[")
                    }

                    matchingReport = next(
                        (
                            r for r in indexData
                            if r.get("status") in ["running", "pending", "created"]
                            and r.get("parameters", {}) == requestedParams
                        ),
                        None,
                    )

                    if matchingReport:
                        if getattr(self.localSetup, "logger", None):
                            self.localSetup.logger.info(
                                "Found active report with matching parameters. Returning its status response."
                            )

                        reportId = matchingReport["id"]
                        statusUrl = f"{p1_apiUrl}/{reportId}"

                        statusResponse, _ = self.makeApiCall(
                            p1_apiUrl=statusUrl,
                            p1_header=p1_header,
                        )
                        return statusResponse, []

                    else:
                        if getattr(self.localSetup, "logger", None):
                            self.localSetup.logger.info(
                                f"409 received but no matching active report with parameters: {requestedParams}. "
                                f"Retrying normally."
                            )

                try:
                    responseObject.close()
                except Exception as closeError:
                    if getattr(self.localSetup, "logger", None):
                        self.localSetup.logger.warning(
                            f"Failed to close API response before retry: {closeError}"
                        )

                if p1_apiCallType.lower() != "delete":
                    raise Exception(f"Failed API call to {p1_apiUrl}: HTTP {statusCode}")
                else:
                    if getattr(self.localSetup, "logger", None):
                        self.localSetup.logger.warning(
                            f"Failed to delete resource at {p1_apiUrl}: HTTP {statusCode}"
                        )
                    return responseObject, []

        ## -------------------------
        ## Pagination (follows RFC 5988 link headers)
        ## -------------------------

        responseObjectList = []

        if hasattr(responseObject, "links") and "next" in getattr(responseObject, "links", {}) and not firstPageOnly:
            responseObjectList.append(responseObject)

            nextUrl = responseObject.links["next"]["url"]
            nextPage, nextPageList = self.makeApiCall(
                p1_apiUrl=nextUrl,
                p1_header=p1_header,
                p1_payload=None,
                p1_files=p1_files,
                p1_apiCallType=p1_apiCallType,
                firstPageOnly=firstPageOnly,
            )

            if nextPageList:
                responseObjectList.extend(nextPageList)
            elif nextPage:
                responseObjectList.append(nextPage)

        return responseObject, responseObjectList


## -------------------------
## Module-level wrapper (backward compatibility)
## -------------------------

def makeApiCall(
    localSetup,
    p1_apiUrl,
    p1_header=None,
    p1_payload=None,
    p1_files=None,
    p1_apiCallType="get",
    firstPageOnly=False,
):
    """
    Backward-compatible module-level wrapper around ApiCaller.makeApiCall.

    Callers that previously imported makeApiCall from TLC_Common can now import
    it from Api_Caller instead (or continue using the re-export in TLC_Common).
    """
    return ApiCaller(localSetup).makeApiCall(
        p1_apiUrl,
        p1_header=p1_header,
        p1_payload=p1_payload,
        p1_files=p1_files,
        p1_apiCallType=p1_apiCallType,
        firstPageOnly=firstPageOnly,
    )