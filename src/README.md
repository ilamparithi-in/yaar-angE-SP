The loop version was created before I got to know about interrupts. It is unnecessarily aggressive on the input detection.

The interrupt version is preferred for usage. Note that for communication over https there are additional changes to be made in the code (described in comments).

Add (uncomment) this before `WiFi.begin()` for stabilising ESP32-C3 Supermini WiFi connection. (Suggestion by [Mario's Ideas](youtube.com/watch?v=IzLD6f8cDHs&t=420))
```cpp
    WiFi.setTxPower(WIFI_POWER_8_5dBm);
```