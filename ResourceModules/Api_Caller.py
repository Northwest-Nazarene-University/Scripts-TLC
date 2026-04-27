## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, time, functools, threading, random, requests, json, copy
from datetime import datetime
from typing import Callable, Tuple, Type, Optional, Dict, Any, List

try: ## If the module is run directly
    from Local_Setup import LocalSetup, logInfo as _logInfo, logWarning as _logWarning, logError
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup, logInfo as _logInfo, logWarning as _logWarning, logError

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
## API token. When dozens of threads fire simultaneously, each one independently
## hits 429 and independently backs off, creating a thundering-herd effect that
## wastes time and generates massive log spam.
##
## Solution:
##   1. _canvasApiSemaphore limits how many threads can be inside the HTTP dispatch
##      at the same time. This prevents overwhelming the bucket in the first place.
##   2. _canvasApiGate is a threading.Event that is normally set (open). When ANY
##      thread receives a 429, it clears the gate (blocking) for the cooldown
##      duration. ALL other threads block on gate.wait() before they can dispatch,
##      so only ONE coordinated pause happens instead of N independent ones.

## Maximum concurrent Canvas API requests. Canvas refills about 10 tokens/second for
## most institutions. Keeping this at the refill rate prevents steady-state 429s.
_canvasMaxConcurrentRequests: int = 10
_canvasApiSemaphore = threading.Semaphore(_canvasMaxConcurrentRequests)

## Global cooldown gate, cleared (blocking) during a 429 cooldown, set (open) normally
_canvasApiGate = threading.Event()
_canvasApiGate.set() ## Start open

_gateLock = threading.Lock()
_gateReopenTime: float = 0.0 ## time.monotonic() when the gate should reopen


## -------------------------
## In-flight dedup tracker (Canvas report/job creation)
## -------------------------
## Goal:
##   Prevent duplicate concurrent "start report/job" calls from multiple threads.
##   Instead of N threads making the same POST/PUT/PATCH and getting 409, only the
##   owner thread dispatches while waiters block and reuse the owner result.
##
## Scope:
##   - Process-local only (shared among threads in this Python process)
##   - Intended for Canvas endpoints that create asynchronous jobs/reports
##   - Uses method + URL + normalized payload as fingerprint
##
## Notes:
##   - We store a response snapshot (status code, headers, body, links, url, reason)
##     rather than sharing a live requests.Response object across threads.
##   - Waiters reconstruct a synthetic requests.Response from snapshot data.
##   - Owner can still hit retries/429/etc. Waiters remain blocked until final outcome.

_inFlightCallsLock = threading.Lock()
_inFlightCalls: Dict[str, Dict[str, Any]] = {}

## Safety timeout for waiting on an existing in-flight entry. Keep aligned with request timeout.
inFlightWaitTimeoutSeconds: float = requestTimeoutSeconds


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

    CHANGED from original: The per-thread sleep on 429 is removed. Instead,
    _triggerGlobalCooldown() blocks ALL threads. The retry loop here just
    catches the exception and loops back to the top of makeApiCall, where the
    gate.wait() will block until the global cooldown expires.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            firstArg = args[0]
            ## Support LocalSetup as first arg or objects with .localSetup (e.g. ApiCaller)
            localSetup = getattr(firstArg, "localSetup", firstArg)

            attempts = 0
            throttleRetries = 0

            currentDelaySeconds = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)

                except RateLimitExceeded as rateLimitError:
                    throttleRetries += 1

                    if getattr(localSetup, "logger", None):
                        _logWarning(
                            localSetup,
                            f"Rate limit hit for {func.__name__} "
                            f"(throttle retry {throttleRetries}/{max_throttle_retries}). "
                            f"Global cooldown triggered - waiting for gate to reopen..."
                        )

                    if throttleRetries >= max_throttle_retries:
                        if getattr(localSetup, "logger", None):
                            logError(
                                localSetup,
                                f"{func.__name__} exceeded max throttle retries ({max_throttle_retries})."
                            )
                        raise

                    ## No per-thread sleep here. The global cooldown (_triggerGlobalCooldown)
                    ## was already initiated by makeApiCall before raising RateLimitExceeded.
                    ## When we loop back, makeApiCall will gate.wait() before dispatching,
                    ## which blocks until the coordinated cooldown expires.

                except exceptions as error:
                    attempts += 1

                    if getattr(localSetup, "logger", None):
                        _logWarning(
                            localSetup,
                            f"Attempt {attempts} failed for {func.__name__}: {error}. "
                            f"Retrying in {currentDelaySeconds:.1f} seconds..."
                        )

                    if attempts == max_attempts:
                        if getattr(localSetup, "logger", None):
                            logError(localSetup, f"{func.__name__} failed after {attempts} attempts.")
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
                    logError(localSetup, f"Failed sending timeout email via {methodName}: {emailError}")

    if getattr(localSetup, "logger", None):
        logError(localSetup, f"No email method found on LocalSetup. Timeout email not sent.\n{subject}\n{body}")


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
    pause briefly before making the next request. This is a best-effort hint;
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
            _logInfo(
                localSetup,
                f"Rate-limit remaining low ({currentRemaining:.1f} <= {rateLimitPauseThreshold:.1f}). "
                f"Preemptive pause {pauseSeconds:.2f}s before {apiUrl}."
            )

        time.sleep(pauseSeconds)


## -------------------------
## Global Cooldown Functions
## -------------------------

def _triggerGlobalCooldown(waitSeconds: float, localSetup=None) -> None:
    """
    Called when ANY thread receives a 429. Closes the gate so ALL threads
    wait until the cooldown expires, then reopens it.

    Only the first thread to trigger the cooldown actually sleeps and reopens
    the gate. Subsequent threads that call this while the gate is already
    closed will see that _gateReopenTime is already far enough in the future
    and return immediately (they will block on _canvasApiGate.wait() instead).
    """
    global _gateReopenTime

    reopenAt = time.monotonic() + waitSeconds

    with _gateLock:
        ## Only extend the cooldown, never shorten it
        if reopenAt <= _gateReopenTime:
            return ## Another thread already set a longer (or equal) cooldown
        _gateReopenTime = reopenAt
        _canvasApiGate.clear() ## Block all threads

    if localSetup and getattr(localSetup, "logger", None):
        _logWarning(
            localSetup,
            f"Global rate-limit cooldown: blocking all Canvas API threads for {waitSeconds:.1f}s"
        )

    ## This thread is responsible for reopening the gate
    time.sleep(waitSeconds)

    with _gateLock:
        ## Only reopen if no other thread extended the cooldown further
        if time.monotonic() >= _gateReopenTime:
            _canvasApiGate.set()
            if localSetup and getattr(localSetup, "logger", None):
                _logInfo(localSetup, "Global rate-limit cooldown expired. Resuming API calls.")


## -------------------------
## In-flight dedup helpers
## -------------------------

def _normalizeForFingerprint(value: Any) -> Any:
    """
    Recursively normalize data so equivalent payloads produce the same JSON string.
    Dict keys are sorted; lists preserve order.
    """
    if isinstance(value, dict):
        return {k: _normalizeForFingerprint(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_normalizeForFingerprint(v) for v in value]
    return value


def _buildInFlightKey(apiUrl: str, apiCallType: str, payload: Optional[Dict[str, Any]]) -> str:
    """
    Fingerprint = METHOD|URL|NORMALIZED_PAYLOAD_JSON
    """
    normalizedPayload = _normalizeForFingerprint(payload or {})
    payloadJson = json.dumps(normalizedPayload, sort_keys=True, separators=(",", ":"), default=str)
    return f"{apiCallType.lower()}|{apiUrl}|{payloadJson}"


def _shouldDedupInFlightCanvasCall(isCanvas: bool, apiCallType: str, apiUrl: str, payload: Optional[Dict[str, Any]]) -> bool:
    """
    Restrict dedup to high-value conflict-prone calls.
    Adjust endpoint conditions as needed for your environment.
    """
    if not isCanvas:
        return False

    method = apiCallType.lower()
    if method not in ["post", "put", "patch"]:
        return False

    loweredUrl = (apiUrl or "").lower()

    ## Conservative report/job-oriented matching
    if "/reports" in loweredUrl or "/report" in loweredUrl:
        return True
    if "/progress/" in loweredUrl:
        return False

    return False


def _snapshotResponse(responseObject: requests.Response) -> Dict[str, Any]:
    """
    Convert requests.Response into a serializable snapshot that can be safely
    shared across threads.
    """
    if responseObject is None:
        return {}

    snapshot: Dict[str, Any] = {
        "status_code": responseObject.status_code,
        "headers": dict(responseObject.headers or {}),
        "content": bytes(responseObject.content or b""),
        "encoding": responseObject.encoding,
        "url": responseObject.url,
        "reason": responseObject.reason,
        "links": copy.deepcopy(getattr(responseObject, "links", {}) or {}),
    }
    return snapshot


def _restoreResponseFromSnapshot(snapshot: Dict[str, Any]) -> requests.Response:
    """
    Build a synthetic requests.Response from a snapshot.
    """
    synthetic = requests.Response()
    synthetic.status_code = int(snapshot.get("status_code", 0) or 0)
    synthetic.headers = requests.structures.CaseInsensitiveDict(snapshot.get("headers", {}) or {})
    synthetic._content = snapshot.get("content", b"") or b""
    synthetic.encoding = snapshot.get("encoding", None)
    synthetic.url = snapshot.get("url", "")
    synthetic.reason = snapshot.get("reason", "")
    synthetic.links = snapshot.get("links", {}) or {}
    return synthetic


def _acquireOrWaitInFlight(localSetup, inFlightKey: str):
    """
    Returns tuple (isOwner, entryOrResultTuple)

    - If owner: (True, entryDict) and caller should execute request.
    - If waiter and owner succeeded: (False, (response, responseList))
    - If waiter and owner failed: raises owner's exception
    """
    with _inFlightCallsLock:
        existing = _inFlightCalls.get(inFlightKey)
        if existing is None:
            entry = {
                "event": threading.Event(),
                "ownerThreadId": threading.get_ident(),
                "startedAtMonotonic": time.monotonic(),
                "waiterCount": 0,
                "responseSnapshot": None,
                "responseListSnapshots": None,
                "exception": None,
            }
            _inFlightCalls[inFlightKey] = entry
            return True, entry

        existing["waiterCount"] += 1
        waitEvent = existing["event"]

    if getattr(localSetup, "logger", None):
        _logInfo(localSetup, "Duplicate in-flight call detected. Waiting for owner result.")

    completed = waitEvent.wait(timeout=inFlightWaitTimeoutSeconds)
    if not completed:
        raise TimeoutError("Timed out waiting for in-flight owner call to complete.")

    with _inFlightCallsLock:
        finished = _inFlightCalls.get(inFlightKey)

    ## Owner may have cleaned up already; in that case waiters should have received data
    ## before cleanup because event set happens first. This is a safety fallback.
    if finished is None:
        raise RuntimeError("In-flight call entry missing after completion signal.")

    if finished.get("exception") is not None:
        raise finished["exception"]

    ownerResponseSnapshot = finished.get("responseSnapshot")
    ownerListSnapshots = finished.get("responseListSnapshots") or []

    if ownerResponseSnapshot is None:
        raise RuntimeError("In-flight owner finished without response snapshot.")

    restoredResponse = _restoreResponseFromSnapshot(ownerResponseSnapshot)
    restoredList = [_restoreResponseFromSnapshot(s) for s in ownerListSnapshots]

    return False, (restoredResponse, restoredList)


def _completeInFlightSuccess(inFlightKey: str, responseObject: requests.Response, responseObjectList: List[requests.Response]) -> None:
    with _inFlightCallsLock:
        entry = _inFlightCalls.get(inFlightKey)
        if entry is None:
            return
        entry["responseSnapshot"] = _snapshotResponse(responseObject)
        entry["responseListSnapshots"] = [_snapshotResponse(r) for r in (responseObjectList or [])]
        entry["event"].set()


def _completeInFlightException(inFlightKey: str, error: Exception) -> None:
    with _inFlightCallsLock:
        entry = _inFlightCalls.get(inFlightKey)
        if entry is None:
            return
        entry["exception"] = error
        entry["event"].set()


def _cleanupInFlight(inFlightKey: str) -> None:
    with _inFlightCallsLock:
        _inFlightCalls.pop(inFlightKey, None)


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
    - Global cooldown gate, when ANY thread gets 429, ALL threads pause together
    - HTTP 429 -> RateLimitExceeded (separate retry lane, does not consume max_attempts)
    - Canvas-specific 409 Conflict handling for report generation
    - Optional in-flight dedup for report/job creation calls to avoid duplicate 409s

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
        - In-flight dedup for report/job creation endpoints to avoid duplicate 409s

        Non-Canvas calls:
        - No default auth header (caller must provide)
        - No rate-limit quota tracking or preemptive pauses
        - HTTP 429 still raises RateLimitExceeded for retry

        Status validation (both):
        - Any 2xx is success
        - 400 is allowed (unchanged behavior)
        - DELETE failures log a warning and return response instead of raising
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

        dedupEnabled = _shouldDedupInFlightCanvasCall(isCanvas, p1_apiCallType, p1_apiUrl, p1_payload)
        inFlightKey: Optional[str] = None
        inFlightOwner: bool = False
        inFlightOwnerEntry: Optional[Dict[str, Any]] = None

        if dedupEnabled:
            inFlightKey = _buildInFlightKey(p1_apiUrl, p1_apiCallType, p1_payload)
            isOwner, ownerOrResult = _acquireOrWaitInFlight(self.localSetup, inFlightKey)
            if not isOwner:
                return ownerOrResult
            inFlightOwner = True
            inFlightOwnerEntry = ownerOrResult

            if getattr(self.localSetup, "logger", None):
                _logInfo(self.localSetup, f"In-flight owner acquired for dedup key: {inFlightKey}")

        try:
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
                    logError(
                        self.localSetup,
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
                            _logWarning(
                                self.localSetup,
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
                            ## Canvas encodes report parameters as "parameters[<key>]" in the payload.
                            ## Strip wrapper to get plain key for matching against stored report parameters.
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
                                _logInfo(
                                    self.localSetup,
                                    "Found active report with matching parameters. Returning its status response."
                                )

                            reportId = matchingReport["id"]
                            statusUrl = f"{p1_apiUrl}/{reportId}"

                            statusResponse, _ = self.makeApiCall(
                                p1_apiUrl=statusUrl,
                                p1_header=p1_header,
                            )
                            responseObject = statusResponse
                            responseObjectList = []

                            if inFlightOwner and inFlightKey:
                                _completeInFlightSuccess(inFlightKey, responseObject, responseObjectList)

                            return responseObject, responseObjectList

                        else:
                            if getattr(self.localSetup, "logger", None):
                                _logInfo(
                                    self.localSetup,
                                    f"409 received but no matching active report with parameters: {requestedParams}. "
                                    f"Retrying normally."
                                )

                    try:
                        responseObject.close()
                    except Exception as closeError:
                        if getattr(self.localSetup, "logger", None):
                            _logWarning(
                                self.localSetup,
                                f"Failed to close API response before retry: {closeError}"
                            )

                    if p1_apiCallType.lower() != "delete":
                        raise Exception(f"Failed API call to {p1_apiUrl}: HTTP {statusCode}")
                    else:
                        if getattr(self.localSetup, "logger", None):
                            _logWarning(
                                self.localSetup,
                                f"Failed to delete resource at {p1_apiUrl}: HTTP {statusCode}"
                            )
                        responseObjectList = []
                        if inFlightOwner and inFlightKey:
                            _completeInFlightSuccess(inFlightKey, responseObject, responseObjectList)
                        return responseObject, responseObjectList

            ## -------------------------
            ## Pagination (follows RFC 5988 link headers)
            ## -------------------------

            responseObjectList: List[requests.Response] = []

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

            if inFlightOwner and inFlightKey:
                _completeInFlightSuccess(inFlightKey, responseObject, responseObjectList)

            return responseObject, responseObjectList

        except Exception as error:
            if inFlightOwner and inFlightKey:
                _completeInFlightException(inFlightKey, error)
            raise

        finally:
            ## Keep entry briefly available long enough for waiters to read snapshots after event set.
            ## Since waiters read from shared dict after wakeup, we avoid immediate pop in success/failure path.
            ## Cleanup when owner exits and no waiters remain OR after event set short grace period.
            if inFlightOwner and inFlightKey:
                ## Short grace sleep allows waiters to wake and restore snapshots.
                time.sleep(0.05)
                _cleanupInFlight(inFlightKey)


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