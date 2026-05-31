#include <Arduino.h>
#include <WiFi.h>
#include <HTTPClient.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include <time.h>

//\\ Debug Logging //\\
// Set to 1 to enable verbose serial diagnostics; 0 for silent production mode.
#define DEBUG_LOG 0
#if DEBUG_LOG
  #define DLOG(fmt, ...)  Serial.printf(fmt, ##__VA_ARGS__)
  #define DLOGLN(s)       Serial.println(s)
  #define DLOGP(s)        Serial.print(s)
#else
  #define DLOG(fmt, ...)  ((void)0)
  #define DLOGLN(s)       ((void)0)
  #define DLOGP(s)        ((void)0)
#endif

//\\ Configuration //\\

// Hardware
static constexpr uint8_t  CPU_FREQ_MHZ            = 80;   // lower frequency reduces idle heat
static constexpr uint8_t  BELL_PIN                = 4;
static constexpr uint8_t  STATUS_LED_PIN          = 2;
// Set to true  if the LED turns on when the pin is HIGH (most external LEDs).
// Set to false if the LED turns on when the pin is LOW  (some built-in LEDs).
static constexpr bool     LED_ACTIVE_HIGH         = true;

// Input timing (milliseconds)
// POLL_INTERVAL_MS: sampling interval used by inputTask while actively polling
// after an interrupt. For AC zero-crossing signals, keep > the AC period so
// each sample sees a stable half-cycle:  50 Hz => > 10 ms,  60 Hz => > 8 ms
static constexpr uint32_t POLL_INTERVAL_MS        = 20;
static constexpr uint32_t DEBOUNCE_MS             = 100;
static constexpr uint32_t REARM_MS                = 300;
static constexpr uint32_t MIN_EVENT_INTERVAL_MS   = 2000;

// Queue
static constexpr UBaseType_t QUEUE_SIZE           = 16;

// Delivery
static constexpr uint8_t  MAX_RETRIES             = 3;
static constexpr uint32_t RETRY_DELAY_MS          = 2000;
static constexpr uint32_t REQUEUE_COOLDOWN_MS     = 30000;

// Time
static const char*        NTP_SERVER              = "time.google.com";
static constexpr uint32_t NTP_RETRY_INTERVAL_MS   = 10000;
static constexpr uint32_t NTP_WAIT_TIMEOUT_MS     = 30000;

// Network credentials - replace before deployment
static const char*        WIFI_SSID               = "airtelblack.com";
static const char*        WIFI_PASSWORD           = "nice_website_lol";
static const char*        BACKEND_URL             = "http://192.168.1.69/iot/callingbell";
static const char*        BEARER_TOKEN            = "purpose_specific_system_write_your_own_code";
static const char*        BELL_LOCATION           = "Sorkkavaasal (Heaven's Entrance)";

//\\ Data Model //\\

struct BellEvent {
    uint32_t      id;
    unsigned long millisAtDetection;  // monotonic capture time (millis())
};

//\\ Shared State between producer and consumer //\\

volatile bool timeSynced       = false;  // wall clock is valid
volatile bool ntpSyncRequested = false;  // ntpTask should attempt sync

QueueHandle_t bellQueue = nullptr;

//\\ Input State //\\

enum BellState { IDLE, ARMED };

static BellState     bellState          = IDLE;
static int           lastRawState       = HIGH;   // pull-up: idle is HIGH
static int           stableState        = HIGH;
static unsigned long lastRawChange      = 0;
static bool          trackingRelease    = false;
static unsigned long releaseStableSince = 0;
static unsigned long lastAcceptedEvent  = 0;      // 0 = no event yet
static uint32_t      nextEventId        = 1;


//\\ LED Feedback //\\

/* Write a logical LED state (true = on) respecting LED_ACTIVE_HIGH. */
static inline void ledWrite(bool on) {
    digitalWrite(STATUS_LED_PIN, (on == LED_ACTIVE_HIGH) ? HIGH : LOW);
}

/**
 * Blink the LED count times, then restore the idle-connected state (OFF).
 * Must only be called from an RTOS task context (uses vTaskDelay).
 */
static void blinkLed(int count, uint32_t onMs, uint32_t offMs) {
    for (int i = 0; i < count; ++i) {
        ledWrite(true);
        vTaskDelay(pdMS_TO_TICKS(onMs));
        ledWrite(false);
        vTaskDelay(pdMS_TO_TICKS(offMs));
    }
}

static void blinkSuccess() { blinkLed(2, 150, 100); }

// Solid ON for 500ms
static void blinkFailure() {
    ledWrite(true);
    vTaskDelay(pdMS_TO_TICKS(500));
    ledWrite(false);
}

// Check out https://github.com/ilamparithi-in/currentu-pochino !
static void startupChime() {
    ledWrite(true);  delay(500);  ledWrite(false);
    for (int i = 0; i < 10; i++) {
        delay(20);  ledWrite(true);  delay(50);  ledWrite(false);
    }
    delay(500);  ledWrite(true);  delay(100);  ledWrite(false);
}

//\\ Event Delivery //\\

/**
 * Reconstruct the event epoch, serialise to JSON, and POST to the backend.
 *
 * Returns true  implies event can be discarded (HTTP 200 / 409 / 401).
 * Returns false implies transient failure; caller should retry.
 *
 * NOTE: For HTTPS, replace http.begin(BACKEND_URL) with
 *       http.begin(wifiClientSecure, BACKEND_URL) and supply a CA certificate.
 */
static bool sendEvent(const BellEvent& event) {
    if (WiFi.status() != WL_CONNECTED) {
        DLOGLN("[DELIVERY] no WiFi - skipping");
        return false;
    }

    // Reconstruct epoch at press time from current wall clock + elapsed millis
    const unsigned long elapsedMs = millis() - event.millisAtDetection;
    const time_t nowEpoch         = time(nullptr);
    const time_t eventEpoch       = nowEpoch - static_cast<time_t>(elapsedMs / 1000);

    char payloadBuf[64];
    snprintf(payloadBuf, sizeof(payloadBuf),
             "{\"id\":%u,\"timestamp\":%ld}",
             event.id, static_cast<long>(eventEpoch));
    const String payload(payloadBuf);

    HTTPClient http;
    http.begin(BACKEND_URL);
    http.addHeader("Content-Type", "application/json");
    http.addHeader("Authorization", String("Bearer ") + BEARER_TOKEN);
    http.addHeader("X-Bell-Location", BELL_LOCATION);
    http.setTimeout(10000);

    const int httpCode = http.POST(payload);
    http.end();

    DLOG("[DELIVERY] id=%u  http=%d\n", event.id, httpCode);

    if (httpCode == 200 || httpCode == 409) {
        // 200 = accepted; 409 = duplicate already recorded - both are success
        blinkSuccess();
        return true;
    }

    if (httpCode == 401) {
        // Credentials will not self-heal; drop event to avoid queue saturation
        DLOGLN("[DELIVERY] 401 auth failure - check BEARER_TOKEN, dropping event");
        blinkFailure();
        return true;
    }

    // All other codes (5xx, network error, timeout) are transient
    blinkFailure();
    return false;
}

//\\ RTOS Task: Network Consumer //\\

/**
 * Pulls events from bellQueue, waits for wall-clock validity, delivers via
 * HTTP with retry, and requeues to the front on max-retry exhaustion.
 */
static void networkTask(void* /*param*/) {
    BellEvent event;

    for (;;) {
        // Block indefinitely until an event arrives
        if (xQueueReceive(bellQueue, &event, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        DLOG("[NETWORK] dequeued id=%u\n", event.id);

        // Defer until wall clock is valid to guarantee correct timestamps
        while (!timeSynced) {
            DLOGLN("[NETWORK] waiting for time sync...");
            vTaskDelay(pdMS_TO_TICKS(1000));
        }

        bool    delivered = false;
        uint8_t retries   = 0;

        while (!delivered && retries < MAX_RETRIES) {
            delivered = sendEvent(event);
            if (!delivered) {
                ++retries;
                DLOG("[NETWORK] retry %u/%u for id=%u\n",
                     retries, MAX_RETRIES, event.id);
                vTaskDelay(pdMS_TO_TICKS(RETRY_DELAY_MS));
            }
        }

        if (!delivered) {
            DLOG("[NETWORK] max retries reached - requeueing id=%u\n", event.id);
            // Push to front so this event is attempted before any newer ones
            if (xQueueSendToFront(bellQueue, &event, 0) != pdTRUE) {
                DLOGLN("[NETWORK] queue full - event dropped");
            }
            // Cool-off before the front-of-queue event is tried again
            vTaskDelay(pdMS_TO_TICKS(REQUEUE_COOLDOWN_MS));
        }
    }
}

//\\ RTOS Task: NTP Synchronisation //\\

/**
 * Watches ntpSyncRequested and drives NTP synchronisation.
 * Runs independently of Wi-Fi callbacks to avoid watchdog issues.
 */

// The implementation in this version is inefficient and has been replaced with a no loop task in the interrupt ver.
static void ntpTask(void* /*param*/) {
    for (;;) {
        if (ntpSyncRequested) {
            ntpSyncRequested = false;
            DLOGLN("[NTP] attempting synchronisation...");

            configTime(0, 0, NTP_SERVER);

            struct tm   timeinfo;
            bool        synced       = false;
            unsigned long attemptStart = millis();

            while ((millis() - attemptStart) < NTP_WAIT_TIMEOUT_MS) {
                if (getLocalTime(&timeinfo, 1000)) {
                    synced = true;
                    break;
                }
                vTaskDelay(pdMS_TO_TICKS(1000));
            }

            if (synced) {
                timeSynced = true;
                DLOG("[NTP] synced - %04d-%02d-%02d %02d:%02d:%02d UTC\n",
                     timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                     timeinfo.tm_hour,        timeinfo.tm_min,      timeinfo.tm_sec);
            } else {
                DLOGLN("[NTP] timed out - will retry");
                ntpSyncRequested = true;  // reschedule
            }
        }

        vTaskDelay(pdMS_TO_TICKS(NTP_RETRY_INTERVAL_MS));
    }
}

//\\ Wi-Fi Event Handler //\\

static void onWiFiEvent(WiFiEvent_t event) {
    switch (event) {
        case ARDUINO_EVENT_WIFI_STA_GOT_IP:
            DLOGP("[WIFI] connected  IP=");
            DLOGLN(WiFi.localIP());
            ledWrite(false);  // connected -> LED OFF
            ntpSyncRequested = true;
            break;

        case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
            DLOGLN("[WIFI] disconnected");
            timeSynced       = false;
            ntpSyncRequested = false;
            ledWrite(true);   // disconnected -> LED ON
            WiFi.reconnect();
            break;

        default:
            break;
    }
}

//\\ Input //\\

static void acceptPress(unsigned long nowMs) {
    BellEvent ev{};
    ev.id                = nextEventId++;
    ev.millisAtDetection = nowMs;
    lastAcceptedEvent    = nowMs;

    DLOG("[INPUT] press accepted  id=%u\n", ev.id);

    // Non-blocking send; drop newest if queue is full rather than stalling input
    if (xQueueSendToBack(bellQueue, &ev, 0) != pdTRUE) {
        DLOGLN("[INPUT] queue full - event dropped");
    }
}

//\\ Input: Debounce + FSM Polling //\\ 

/**
 * Called from the main Arduino loop (~1 kHz).
 *   1. Debounce window    - raw level must be stable for DEBOUNCE_MS.
 *   2. FSM (IDLE/ARMED)   - one event per press, hold does not repeat.
 *   3. Rearm delay        - stable release must persist for REARM_MS.
 *   4. Global rate cap    - MIN_EVENT_INTERVAL_MS between accepted events.
 *
 * Active-low convention: LOW = pressed, HIGH = released.
 */

// This would later prove to me that this was useless and too aggressive to work properly.
static void processBellInput() {
    const int          rawState = digitalRead(BELL_PIN);
    const unsigned long now     = millis();

    //\\ 1. Debounce
    if (rawState != lastRawState) {
        lastRawState  = rawState;
        lastRawChange = now;
    }

    if ((now - lastRawChange) >= DEBOUNCE_MS && rawState != stableState) {
        stableState = rawState;
    }

    //\\ 2 & 3. Finite State Machine
    if (bellState == IDLE) {
        if (stableState == LOW) {
            //\\ 4. Global rate cap
            const bool rateOk =
                (lastAcceptedEvent == 0) ||
                ((now - lastAcceptedEvent) >= MIN_EVENT_INTERVAL_MS);

            if (rateOk) {
                acceptPress(now);
                bellState = ARMED;
            }
        }

    } else {  // ARMED - waiting for a clean release before rearming
        if (stableState == HIGH) {
            if (!trackingRelease) {
                trackingRelease    = true;
                releaseStableSince = now;
            }

            if ((now - releaseStableSince) >= REARM_MS) {
                bellState       = IDLE;
                trackingRelease = false;
            }
        } else {
            // Re-pressed before rearm elapsed - reset release window
            trackingRelease = false;
        }
    }
}

//\\ Setup //\\

void setup() {
    setCpuFrequencyMhz(CPU_FREQ_MHZ);
    Serial.begin(115200);
    delay(200);
    Serial.println("\n=== Doorbell Monitor Starting ===");
    Serial.printf("CPU = %lu MHz\n", getCpuFrequencyMhz());

    // GPIO
    pinMode(BELL_PIN,       INPUT_PULLUP);
    pinMode(STATUS_LED_PIN, OUTPUT);
    startupChime();
    ledWrite(true);  // start disconnected -> LED ON

    // Seed input state from current pin level to avoid false trigger on boot
    stableState  = digitalRead(BELL_PIN);
    lastRawState = stableState;

    // Shared queue
    bellQueue = xQueueCreate(QUEUE_SIZE, sizeof(BellEvent));
    if (bellQueue == nullptr) {
        Serial.println("FATAL: failed to create bellQueue");
        for (;;) { delay(1000); }
    }

    // Wi-Fi (callback registered before begin so no events are missed)
    WiFi.onEvent(onWiFiEvent);
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(true);  // modem sleep reduces radio power and heat
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    DLOGLN("[WIFI] connecting...");

    // RTOS tasks
    xTaskCreate(ntpTask,     "ntpTask",     4096, nullptr, 1, nullptr);
    xTaskCreate(networkTask, "networkTask", 8192, nullptr, 2, nullptr);

    Serial.println("=== Setup complete ===");
}

//\\ Main Loop (Input Handling) //\\ 

void loop() {
    processBellInput();
    delay(POLL_INTERVAL_MS);  // must be > AC period (> 10 ms for 50 Hz, > 8 ms for 60 Hz)
}
