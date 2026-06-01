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
// Minimum time to wait for before deeming that the signal is stably deactivated
static constexpr uint32_t RELEASE_STABLE_MS       = 2000;

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

QueueHandle_t bellQueue       = nullptr;
TaskHandle_t  ntpTaskHandle   = nullptr;
TaskHandle_t  inputTaskHandle = nullptr;

//\\ Input State //\\

enum BellState { READY, LOCKED };

static BellState bellState = READY;

static bool releaseTiming = false;
static unsigned long releaseStableSince = 0;
static unsigned long lastAcceptedEvent  = 0; // 0 = no event yet
static uint32_t      nextEventId        = 1;

//\\ LED Feedback //\\

/** Write a logical LED state (true = on) respecting LED_ACTIVE_HIGH. */
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
static void ntpTask(void* /*param*/) {
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        DLOGLN("[NTP] attempting synchronisation...");

        configTime(0, 0, NTP_SERVER);

        struct tm timeinfo;
        bool          synced       = false;
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
            // reschedule
            vTaskDelay(pdMS_TO_TICKS(NTP_RETRY_INTERVAL_MS));
            xTaskNotifyGive(ntpTaskHandle);
        }
    }
}

//\\ Wi-Fi Event Handler //\\

static void onWiFiEvent(WiFiEvent_t event) {
    switch (event) {
        case ARDUINO_EVENT_WIFI_STA_GOT_IP:
            DLOGP("[WIFI] connected  IP=");
            DLOGLN(WiFi.localIP());
            ledWrite(false);  // connected -> LED OFF
            xTaskNotifyGive(ntpTaskHandle);
            break;

        case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
            DLOGLN("[WIFI] disconnected");
            timeSynced       = false;
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

static void IRAM_ATTR bellISR() {
    if (bellState != READY) return;

    bellState = LOCKED;
    // ^ detachInterrupt(digitalPinToInterrupt(BELL_PIN));
    BaseType_t higherPriorityTaskWoken = pdFALSE;

    vTaskNotifyGiveFromISR(inputTaskHandle, &higherPriorityTaskWoken);
    portYIELD_FROM_ISR(higherPriorityTaskWoken);
}

static void inputTask(void* /*param*/) {
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        acceptPress(millis());
        releaseTiming = false;

        while (bellState == LOCKED) {
            const int state = digitalRead(BELL_PIN);

            if (state == HIGH) {
                if (!releaseTiming) {
                    releaseTiming = true;
                    releaseStableSince = millis();
                }
                if (millis() - releaseStableSince >= RELEASE_STABLE_MS) {
                    bellState = READY;
                    // ^ attachInterrupt(digitalPinToInterrupt(BELL_PIN), bellISR, FALLING);
                    releaseTiming = false;
                }
            } else {
                releaseTiming = false;
            }

            vTaskDelay(pdMS_TO_TICKS(10));
        }
    }
}

//\\ Setup //\\

void setup() {
    setCpuFrequencyMhz(CPU_FREQ_MHZ);
    Serial.begin(115200);
    delay(200);
    Serial.println("\n=== Calling Bell Monitor Starting ===");
    Serial.printf("CPU = %lu MHz\n", getCpuFrequencyMhz());

    // GPIO
    pinMode(BELL_PIN,       INPUT_PULLUP);
    pinMode(STATUS_LED_PIN, OUTPUT);
    startupChime();
    ledWrite(true);  // start disconnected -> LED ON

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
    // WiFi.setTxPower(WIFI_POWER_8_5dBm); // for ESP32-C3 Supermini
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    DLOGLN("[WIFI] connecting...");

    // RTOS tasks
    xTaskCreate(ntpTask,     "ntpTask",     4096, nullptr, 1, &ntpTaskHandle);
    xTaskCreate(networkTask, "networkTask", 8192, nullptr, 2, nullptr);
    xTaskCreate(inputTask,   "inputTask",   2048, nullptr, 3, &inputTaskHandle);

    // Attach interrupt only after inputTaskHandle is valid
    attachInterrupt(digitalPinToInterrupt(BELL_PIN), bellISR, FALLING);

    Serial.println("=== Setup complete ===");
}

//\\ Main Loop (Arduino Requirement) //\\

void loop() {
    vTaskDelay(pdMS_TO_TICKS(1000));  // input is interrupt-driven; main loop is idle
}
