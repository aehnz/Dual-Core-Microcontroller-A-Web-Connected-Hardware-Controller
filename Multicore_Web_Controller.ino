/*
  Multicore Web-Connected Hardware Controller v3 - ESP32-S3

  CORE 0 (priority 2): Wi-Fi, HTTP :80, WebSockets :81, command producer,
                       per-core CPU sampling, trace streaming
  CORE 1 (priority 4): periodic 10 ms hardware loop, command consumer
                       (set HARDWARE_CORE to 0 to run it beside Wi-Fi and compare)

  Cross-core communication (no mutex anywhere):
    Core 0 -> Core 1 : FreeRTOS queue (commands), atomic "last activity" timestamp
    Core 1 -> Core 0 : seqlock snapshot (state) + SPSC ring buffer (trace events)
    Network counters : owned by Core 0 only (all handlers run in networkTask)

  What the dashboard shows per core:
    - CPU load per core (from the FreeRTOS idle-task run time)
    - Every task, which core it is pinned to, priority, state, CPU %, free stack
    - A live two-lane timeline: HTTP/WS work on the network core, 10 ms ticks on
      the control core, and a line from each queued command to where it was applied
    - Experiments: CPU hog on either core, flash write (pauses both cores), HTTP stress

  HTTP API (state-changing calls are POST + X-Api-Key header):
    GET  /                      dashboard
    GET  /api/status            JSON metrics (read-only, no token)
    GET  /api/tasks             per-core load + task table (read-only, no token)
    GET  /api/trace?since=N     timeline events after N (fallback when ws:// is blocked)
    POST /api/relay?relay=1&state=on
    POST /api/all?state=off
    POST /api/reset             reset max-jitter / histogram / max latency
    POST /api/ping              keepalive (feeds the failsafe)
    POST /api/hog?core=0&ms=500 spin one core at priority 5 for 50..2000 ms
    POST /api/flashtest         write 4 KB to NVS (shows the cross-core flash stall)
  WebSocket ws://DEVICE_IP:81/  (send AUTH:<token> first for commands)
    AUTH:token, R1:ON, R1:OFF, ALL:ON, ALL:OFF, RESET, PING, STATUS, HOG:<core>:<ms>, FLASH
    Server pushes {"type":"status"|"tasks"|"trace", ...}

  Credentials go in secrets.h next to this file (already in .gitignore):
    #define WIFI_SSID     "your-ssid"
    #define WIFI_PASSWORD "your-password"
    #define API_TOKEN     "long-random-string"

  Libraries: "WebSockets" by Markus Sattler (Links2004).
  Board: ESP32S3 Dev Module, Arduino-ESP32 core 3.x.
  Virtual relay mode by default.
  This is a timing-isolation demonstration, NOT a hard real-time safety system.
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WebServer.h>
#include <WebSocketsServer.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <esp_timer.h>
#include <esp_task_wdt.h>
#include <esp_system.h>
#include <esp_idf_version.h>
#include <atomic>
#include <stdarg.h>
#include <string.h>
#include <strings.h>

#if __has_include("secrets.h")
#include "secrets.h"
#endif
#ifndef WIFI_SSID
#define WIFI_SSID "your-ssid"
#endif
#ifndef WIFI_PASSWORD
#define WIFI_PASSWORD "your-password"
#endif
#ifndef API_TOKEN
#define API_TOKEN "change-me"
#endif

// ---------- Options ----------
#define ENABLE_PHYSICAL_RELAYS 0   // 0 = virtual simulation (safe default)
#define ENABLE_TIMING_GPIO     0   // 1 = toggle GPIO16 every control cycle
#define ENABLE_HEARTBEAT_LED   0   // 1 = blink the on-board RGB LED (GPIO48 on most S3 DevKits) from the control loop
#define RELAY_ACTIVE_HIGH      1   // 0 for active-low relay boards
#define REQUIRE_AUTH           1   // 0 disables token checks (lab use only)

// Which core runs the 10 ms control loop. 1 = isolated from Wi-Fi (default).
// 0 = shares the core with Wi-Fi/lwIP/HTTP, for a before/after comparison.
#ifndef HARDWARE_CORE
#define HARDWARE_CORE 1
#endif
#define NETWORK_CORE  0
static_assert(HARDWARE_CORE == 0 || HARDWARE_CORE == 1, "HARDWARE_CORE must be 0 or 1");

constexpr UBaseType_t NETWORK_PRIORITY  = 2;
constexpr UBaseType_t HARDWARE_PRIORITY = 4;
constexpr UBaseType_t HOG_PRIORITY      = 5;   // above the control loop on purpose

// If > 0: all relays switch OFF when no authenticated activity (command,
// ping, WS ping) arrives for this many ms. 0 = disabled.
constexpr uint32_t FAILSAFE_TIMEOUT_MS = 0;

constexpr uint8_t  TOTAL_RELAYS = 4;
const uint8_t      RELAY_PINS[TOTAL_RELAYS] = {4, 5, 6, 7};
constexpr uint8_t  TIMING_PIN = 16;

constexpr uint32_t HARDWARE_PERIOD_MS     = 10;
constexpr uint32_t NETWORK_POLL_MS        = 2;
constexpr uint32_t WIFI_MONITOR_MS        = 250;
constexpr uint32_t WIFI_RETRY_MS          = 15000;
constexpr uint32_t STATUS_CHECK_MS        = 100;   // WS push: check for changes
constexpr uint32_t STATUS_HEARTBEAT_MS    = 1000;  // WS push: periodic refresh
constexpr uint32_t TASK_SAMPLE_MS         = 1000;  // per-core load / task table
// Reading the task table holds the FreeRTOS kernel lock (shared by both cores)
// for ~2 ms. Measured on v3.0: when that overlapped a control tick, the tick
// slipped by one 1 ms RTOS tick. So the read is started only inside this
// window right after a tick, where it finishes long before the next one.
constexpr int64_t  SAMPLE_WINDOW_US       = 4000;
constexpr int64_t  SAMPLE_STALL_US        = 50000; // no tick for this long = loop stalled, nothing to disturb
constexpr uint32_t TRACE_FLUSH_MS         = 100;   // trace batch to WS clients
constexpr uint32_t HOG_MIN_MS             = 50;
constexpr uint32_t HOG_MAX_MS             = 2000;  // stays well under the 5 s task watchdog
constexpr uint8_t  MAX_COMMANDS_PER_TICK  = 3;
constexpr uint8_t  QUEUE_LENGTH           = 10;
constexpr uint8_t  HIST_BUCKETS           = 6;     // <=50,100,250,500,1000,>1000 us
const uint32_t     HIST_EDGES_US[HIST_BUCKETS - 1] = {50, 100, 250, 500, 1000};
constexpr uint8_t  MAX_WS_CLIENTS         = WEBSOCKETS_SERVER_CLIENT_MAX;
constexpr int      MAX_TASKS              = 40;
constexpr size_t   TRACE_RING_SIZE        = 256;   // Core 1 -> Core 0, ~2.5 s of ticks
constexpr int      CORE0_TRACE_MAX        = 96;
constexpr uint32_t TRACE_HISTORY          = 512;   // recent events kept for HTTP clients
#define HOSTNAME "esp32-controller"

#define U(x) ((unsigned long)(x))

// ---------- Types ----------
enum CommandType : uint8_t { RELAY_CONTROL, ALL_RELAYS_ON, ALL_RELAYS_OFF, RESET_STATS };

struct Command {
  CommandType type;
  uint8_t     relay;        // zero-based
  bool        state;
  uint32_t    id;
  int64_t     enqueuedUs;   // esp_timer_get_time() at producer
};

// Written by the hardware task only, read by Core 0 through the seqlock.
struct HwSnapshot {
  bool     relays[TOTAL_RELAYS];
  uint8_t  core;                 // core the control loop actually runs on
  bool     wdtArmed;             // subscribed to the task watchdog
  int64_t  lastWakeUs;           // esp_timer time of the latest control tick
  uint32_t version;              // increments on every state change
  uint32_t ticks;
  uint32_t processed;
  uint32_t rejected;
  uint32_t totalOperations;
  uint32_t lastIntervalUs;
  uint32_t lastJitterUs;
  uint32_t maxJitterUs;
  uint32_t recentMaxJitterUs;    // max over the last 1-2 s
  uint32_t lateIntervals;        // interval > 1.5x period
  uint32_t lastLatencyUs;
  uint32_t maxLatencyUs;
  uint32_t maxWorkUs;
  uint32_t recentMaxWorkUs;
  uint32_t failsafeTrips;
  uint32_t traceLost;            // ring full (Core 0 did not drain in time)
  uint32_t jitterHist[HIST_BUCKETS];
};

// Single-writer / multi-reader lock-free snapshot.
// (Plain copy of POD data guarded by a sequence counter + fences.)
struct SeqLock {
  std::atomic<uint32_t> seq{0};
  HwSnapshot data{};

  void publish(const HwSnapshot &s) {            // hardware task only
    uint32_t v = seq.load(std::memory_order_relaxed);
    seq.store(v + 1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    memcpy((void *)&data, &s, sizeof(HwSnapshot));
    seq.store(v + 2, std::memory_order_release);
  }
  bool read(HwSnapshot &out) const {             // any reader
    for (int attempt = 0; attempt < 8; ++attempt) {
      uint32_t s1 = seq.load(std::memory_order_acquire);
      if (s1 & 1) continue;
      memcpy(&out, (const void *)&data, sizeof(HwSnapshot));
      std::atomic_thread_fence(std::memory_order_acquire);
      if (seq.load(std::memory_order_relaxed) == s1) return true;
    }
    return false;
  }
};

// ---------- Trace events (what each core is doing, timestamped) ----------
enum EvKind : uint8_t {
  EV_TICK, EV_APPLY, EV_FAILSAFE,                       // control core
  EV_HTTP, EV_WS, EV_ENQ, EV_DROP, EV_PUSH,             // network core
  EV_HOG, EV_FLASH, EV_SAMPLE                           // experiments / sampling
};

struct TraceEvt {
  int64_t  tUs;     // esp_timer time (same clock on both cores)
  uint32_t durUs;   // span length (EV_APPLY: command latency)
  uint32_t id;      // command id / tick number / misc
  uint16_t aux;     // EV_TICK: jitter us, EV_HTTP: route, EV_PUSH: kind ...
  uint8_t  kind;
  uint8_t  core;
};

// Single-producer / single-consumer lock-free ring (hardware task -> networkTask).
template <size_t N>
struct SpscRing {
  TraceEvt buf[N];
  std::atomic<uint32_t> head{0}, tail{0};

  bool push(const TraceEvt &e) {                 // producer only
    uint32_t h = head.load(std::memory_order_relaxed);
    if (h - tail.load(std::memory_order_acquire) >= N) return false;
    buf[h % N] = e;
    head.store(h + 1, std::memory_order_release);
    return true;
  }
  bool pop(TraceEvt &e) {                        // consumer only
    uint32_t t = tail.load(std::memory_order_relaxed);
    if (t == head.load(std::memory_order_acquire)) return false;
    e = buf[t % N];
    tail.store(t + 1, std::memory_order_release);
    return true;
  }
};

// Core 0 only.
struct NetStats {
  uint32_t executions;
  uint32_t rootRequests;
  uint32_t statusRequests;
  uint32_t commandsQueued;
  uint32_t commandsRejected;
  uint32_t queueDrops;
  uint32_t queueHighWater;
  uint32_t authFailures;
  uint32_t loopLastUs;
  uint32_t loopMaxUs;
};

struct TaskRow {
  char     name[configMAX_TASK_NAME_LEN];
  int8_t   core;          // 0, 1, or -1 = not pinned
  uint8_t  prio;
  char     state;         // R running, Y ready, B blocked, S suspended, D deleted
  uint32_t stackFree;     // bytes
  uint16_t cpuPermille;   // of one core, over the last sample period
};

// Small bounded JSON writer; format strings are checked by the compiler.
struct JBuf {
  char  *p;
  size_t cap;
  size_t len = 0;
  bool   ovf = false;
  JBuf(char *buf, size_t c) : p(buf), cap(c) { if (cap) p[0] = '\0'; }
  void add(const char *fmt, ...) __attribute__((format(printf, 2, 3))) {
    if (ovf) return;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(p + len, cap - len, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= cap - len) { ovf = true; p[len] = '\0'; return; }
    len += (size_t)n;
  }
};

// ---------- Globals ----------
TaskHandle_t  networkTaskHandle  = nullptr;
TaskHandle_t  hardwareTaskHandle = nullptr;
QueueHandle_t commandQueue       = nullptr;

SeqLock                     hwSnap;
SpscRing<TRACE_RING_SIZE>   traceRing;
NetStats                    net = {};
std::atomic<uint32_t>       lastActivityMs{0};
esp_reset_reason_t          bootReason = ESP_RST_UNKNOWN;

// CPU hog experiment (any core); handed back to networkTask through atomics.
std::atomic<bool>     hogBusy{false};
std::atomic<bool>     hogDone{false};
volatile uint32_t     hogMs = 0;
volatile uint8_t      hogCore = 0;
volatile int64_t      hogStartUs = 0;
volatile int64_t      hogEndUs = 0;

// Hardware task only
bool relayState[TOTAL_RELAYS] = {};

// Core 0 only
uint32_t nextCommandID = 1;
bool     wifiUp = false;
bool     serversRunning = false;
uint32_t lastWifiRetry = 0;
bool     wsAuthed[MAX_WS_CLIENTS] = {};
char     jsonBuf[2048];
char     tasksBuf[3072];
char     traceBuf[8192];

TraceEvt core0Trace[CORE0_TRACE_MAX];
uint16_t core0TraceCount = 0;
uint32_t core0TraceLost = 0;

// Recent events from both cores, for clients that cannot open the WebSocket
// (some proxies and embedded browsers block ws://). Served by GET /api/trace.
TraceEvt traceHist[TRACE_HISTORY];
uint32_t traceHistSeq = 0;      // total events ever recorded

TaskStatus_t                taskStat[MAX_TASKS];
TaskHandle_t                prevHandle[MAX_TASKS];
configRUN_TIME_COUNTER_TYPE prevRun[MAX_TASKS];
int                         prevCount = 0;
configRUN_TIME_COUNTER_TYPE prevTotal = 0;
TaskRow                     taskRows[MAX_TASKS];
int                         taskRowCount = 0;
uint8_t                     coreLoad[2] = {0, 0};
uint32_t                    lastSampleUs = 0;
float                       chipTempC = 0;

Preferences prefs;
uint32_t    flashTests = 0;
uint32_t    lastFlashUs = 0;

WebServer         httpServer(80);
WebSocketsServer  webSocket(81);

// -------------------------------------------------------------------
// Dashboard (flash)
// -------------------------------------------------------------------
const char dashboardHTML[] PROGMEM = R"HTML(
<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ESP32-S3 Multicore</title>
<style>
:root{color-scheme:dark;--bg:#0b1120;--panel:#131d33;--line:#26375a;--text:#e5edf8;--dim:#93a6c2;
--ok:#4fd6a0;--warn:#f2b54b;--bad:#ff7a7a;--c0:#5aa2ff;--c1:#b48cff;--http:#5aa2ff;--ws:#46c7e0;--push:#6b84a6;
--enq:#ffd166;--apply:#4fd6a0;--hog:#f2b54b;--flash:#ff7a7a;font-family:system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif}
*{box-sizing:border-box}body{background:var(--bg);color:var(--text);margin:0;padding:16px}
main{max-width:1180px;margin:auto}h1{margin:4px 0;font-size:clamp(20px,3.6vw,30px)}h2{margin:0;font-size:18px}h3{margin:0 0 10px;font-size:15px}
.sub{color:var(--dim);margin:4px 0 8px;font-size:14px}.mini{font-size:12.5px;color:var(--dim);margin:2px 0}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px;margin-top:12px;min-width:0}
.row{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.cores{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(330px,100%),1fr));gap:12px}
.kicker{font-size:12px;letter-spacing:.08em;color:var(--dim);font-weight:600}
.big{font-size:34px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1}.big small{font-size:13px;color:var(--dim);font-weight:400;margin-left:4px}
.meter{height:8px;background:#0d1628;border-radius:4px;overflow:hidden;margin:10px 0 6px}.meter i{display:block;height:100%;width:0;transition:width .4s}
#m0{background:var(--c0)}#m1{background:var(--c1)}#core0{border-top:3px solid var(--c0)}#core1{border-top:3px solid var(--c1)}
canvas{display:block;width:100%}.spark{height:44px;margin-bottom:4px}
.kv{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:4px 14px;margin:10px 0;font-size:13px}
.kv div{display:flex;justify-content:space-between;gap:8px;border-bottom:1px solid var(--line);padding:3px 0}
.kv span{color:var(--dim)}.kv b{font-weight:600;font-variant-numeric:tabular-nums;text-align:right;overflow-wrap:anywhere}
.tw{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:12.5px;font-variant-numeric:tabular-nums}
th{text-align:left;color:var(--dim);font-weight:500;padding:4px 6px;border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:3px 6px;border-bottom:1px solid #1b2843;white-space:nowrap;color:#c4d2e4}
td:nth-child(n+2),th:nth-child(n+2){text-align:right}tr.mine td{color:var(--text);font-weight:700}
.cpu{display:inline-block;height:6px;background:var(--dim);border-radius:3px;vertical-align:middle;margin-right:6px}
button,select,input{background:#2b4166;color:#fff;border:1px solid #4c6890;padding:8px 12px;border-radius:8px;font-size:14px}
button{cursor:pointer}button:hover{background:#38557f}button:disabled{opacity:.5;cursor:default}
button.on{background:#14634d}button.off{background:#6d3546}button.ghost{background:transparent}
input{min-width:200px}
#tl{height:230px;background:#0d1628;border-radius:8px}
.legend{display:flex;flex-wrap:wrap;gap:6px 16px;font-size:12px;color:var(--dim);margin-top:8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:-1px}
.journey{margin-top:8px;font-size:13px;min-height:1.4em}
.hb{display:grid;grid-template-columns:64px 1fr 70px;align-items:center;gap:8px;font-size:12.5px;margin:4px 0}
.hb span{color:var(--dim)}.hb b{text-align:right;font-weight:500;font-variant-numeric:tabular-nums}
.hb i{display:block;height:10px;border-radius:5px}.g{background:var(--ok)}.y{background:var(--warn)}.r{background:var(--bad)}
.exp{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;padding:10px 0;border-top:1px solid var(--line)}
.exp:first-of-type{border-top:0}.exp>div:first-child{flex:1 1 320px}.ctl{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.relay{padding:8px 0;border-bottom:1px solid var(--line)}.relay:last-child{border-bottom:0}
.pill{display:inline-block;font-size:11px;padding:2px 8px;border-radius:10px;background:#2a3550;color:var(--dim);margin-left:6px}.pill.on{background:#14634d;color:#fff}
.two{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(330px,100%),1fr));gap:12px}
#log{height:120px;overflow:auto;white-space:pre-wrap;font:12px ui-monospace,Menlo,monospace;color:#b2c3d5}
</style></head>
<body><main>
<h1>ESP32-S3 Multicore Controller</h1>
<p class="sub">Commands go in through a queue, state comes out through a lock-free snapshot, and every event is timestamped on both cores.</p>
<section class="panel row"><span id="conn" class="mini">Connecting...</span>
<input id="tok" type="password" placeholder="API token (for commands)" autocomplete="off"></section>

<div class="cores">
<section class="panel" id="core0"><div class="row"><div><div class="kicker">CORE 0</div><h2 id="r0">-</h2></div>
<div class="big"><span id="l0">-</span><small>% busy</small></div></div>
<div class="meter"><i id="m0"></i></div><canvas id="sp0" class="spark"></canvas><div class="mini">CPU load, last 60 s</div>
<div class="kv" id="kv0"></div>
<div class="tw"><table><thead><tr><th>Task pinned to core 0</th><th>Prio</th><th>State</th><th>CPU</th><th>Stack free</th></tr></thead><tbody id="tb0"></tbody></table></div></section>
<section class="panel" id="core1"><div class="row"><div><div class="kicker">CORE 1</div><h2 id="r1">-</h2></div>
<div class="big"><span id="l1">-</span><small>% busy</small></div></div>
<div class="meter"><i id="m1"></i></div><canvas id="sp1" class="spark"></canvas><div class="mini">CPU load, last 60 s</div>
<div class="kv" id="kv1"></div>
<div class="tw"><table><thead><tr><th>Task pinned to core 1</th><th>Prio</th><th>State</th><th>CPU</th><th>Stack free</th></tr></thead><tbody id="tb1"></tbody></table></div></section>
</div>

<section class="panel"><div class="row"><strong>Live timeline</strong>
<span class="ctl"><select id="win"><option value="1000">1 s</option><option value="2000" selected>2 s</option><option value="5000">5 s</option></select>
<button id="pause" class="ghost">Pause</button></span></div>
<p class="mini">Real events from both cores on one clock. Send a relay command to see it cross from Core 0 to Core 1.</p>
<canvas id="tl"></canvas>
<div class="legend"><span><i style="background:var(--ok)"></i>tick, on time</span><span><i style="background:var(--warn)"></i>tick, late</span><span><i style="background:var(--bad)"></i>tick, very late</span>
<span><i style="background:var(--http)"></i>HTTP request</span><span><i style="background:var(--ws)"></i>WebSocket message</span><span><i style="background:var(--push)"></i>push / sampling</span>
<span><i style="background:var(--enq)"></i>command queued</span><span><i style="background:var(--apply)"></i>command applied</span><span><i style="background:var(--hog)"></i>CPU hog</span><span><i style="background:var(--flash)"></i>flash write</span></div>
<div class="journey" id="journey"></div></section>

<section class="panel"><h3>Experiments</h3>
<div class="exp"><div><b>CPU hog</b><p class="mini">Spins one core at priority 5, above the control loop's 4. On the network core the control loop should not notice. On the control core, ticks stall and jitter jumps.</p></div>
<div class="ctl"><select id="hogCore"><option value="0">Core 0</option><option value="1">Core 1</option></select>
<select id="hogMs"><option>200</option><option selected>500</option><option>1000</option></select><button onclick="hog()">Run hog</button></div></div>
<div class="exp"><div><b>Flash write</b><p class="mini">Writes 4 KB to NVS. While flash is written the CPU cache is off and the other core is parked, so <i>both</i> cores pause. Watch the jitter.</p></div>
<div class="ctl"><button onclick="cmd('FLASH')">Write flash</button></div></div>
<div class="exp"><div><b>HTTP stress</b><p class="mini">Three parallel workers fetch the dashboard. The network core fills up; control ticks should stay evenly spaced.</p></div>
<div class="ctl"><select id="num"><option>20</option><option selected>100</option><option>500</option></select><button id="stress" onclick="stress()">Run</button><span id="result" class="mini">Idle</span></div></div>
<div class="exp"><div><b>Reset stats</b><p class="mini">Clears max jitter, the histogram, max latency and max work.</p></div>
<div class="ctl"><button onclick="cmd('RESET')">Reset</button></div></div></section>

<div class="two">
<section class="panel"><h3>Control-loop jitter</h3><canvas id="spj" class="spark"></canvas><div class="mini">Worst jitter per second, last 60 s</div><div id="hist" style="margin-top:10px"></div></section>
<section class="panel"><h3>Relays</h3><div id="relays"></div>
<div style="margin-top:12px;display:flex;gap:8px"><button class="on" onclick="cmd('ALL:ON')">All ON</button><button class="off" onclick="cmd('ALL:OFF')">All OFF</button></div>
<p class="mini" style="margin-top:10px">Virtual relays unless enabled in the sketch.</p></section>
</div>

<section class="panel"><div class="row"><h3>Unpinned tasks</h3><span class="mini" id="sampleUs"></span></div>
<p class="mini">Not tied to a core; the scheduler runs them wherever there is room.</p>
<div class="tw"><table><thead><tr><th>Task</th><th>Prio</th><th>State</th><th>CPU</th><th>Stack free</th></tr></thead><tbody id="tbu"></tbody></table></div></section>

<div class="two">
<section class="panel"><h3>System</h3><div class="kv" id="sys"></div></section>
<section class="panel"><h3>Event log</h3><div id="log"></div></section>
</div>
</main>
<script>
const $=id=>document.getElementById(id);
const COL={};'ok warn bad c0 c1 http ws push enq apply hog flash dim line text'.split(' ').forEach(k=>COL[k]=getComputedStyle(document.documentElement).getPropertyValue('--'+k).trim());
const ROUTE=['GET /','GET status','POST relay','POST all','POST reset','POST ping','GET tasks','POST hog','POST flash','404','GET trace'];
const STATE={R:'running',Y:'ready',B:'blocked',S:'suspended',D:'deleted'};
const MINE=['NetworkTask','HardwareTask','CpuHog'];
const HL=['<=50 µs','<=100 µs','<=250 µs','<=500 µs','<=1 ms','>1 ms'];
let S=null,ws=null,wsOpen=false,httpLive=false,traceSeq=0,traceBusy=false,paused=false,pauseAt=0,win=2000,evs=[],trims=0;
const ser={l0:[],l1:[],jit:[]};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtB=b=>b>=1048576?(b/1048576).toFixed(1)+' MB':b>=1024?(b/1024).toFixed(1)+' KB':b+' B';
const fmtUp=ms=>{let s=Math.floor(ms/1000),h=Math.floor(s/3600),m=Math.floor(s%3600/60);return (h?h+'h ':'')+(h||m?m+'m ':'')+s%60+'s'};
function log(s){const e=$('log');e.textContent=new Date().toLocaleTimeString()+'  '+s+'\n'+e.textContent.slice(0,4000)}
function kv(el,p){el.innerHTML=p.map(x=>`<div><span>${x[0]}</span><b>${esc(x[1])}</b></div>`).join('')}
function role(c){if(!S)return'-';const r=[];if(c===S.netCore)r.push('Network');if(c===S.hwCore)r.push('Control loop');return r.join(' + ')||'System tasks'}
function setLoad(c,v){$('l'+c).textContent=v;$('m'+c).style.width=v+'%'}
function push(a,v){a.push(v);if(a.length>60)a.shift()}
for(let i=1;i<=4;i++)$('relays').insertAdjacentHTML('beforeend',`<div class="row relay"><span><strong>Relay ${i}</strong><span class="pill" id="s${i}">-</span></span><span class="ctl"><button class="on" onclick="cmd('R${i}:ON')">ON</button><button class="off" onclick="cmd('R${i}:OFF')">OFF</button></span></div>`);

function renderStatus(d){
 S=d;
 for(let c=0;c<2;c++)$('r'+c).textContent=role(c);
 const net=[['HTTP GET /',d.rootRequests],['Status polls',d.statusRequests],['WS clients',d.wsClients],['Net loop max',d.netLoopMaxUs+' µs'],
  ['Queue',d.queueDepth+'/'+d.queueLength+' (peak '+d.queueHighWater+')'],['Queue drops',d.queueDrops],['Auth failures',d.authFailures],['Net stack free',d.netStackFree+' B']];
 const hw=[['Control ticks',d.hardwareTicks],['Jitter, last 1 s',d.recentMaxJitterUs+' µs'],['Max jitter',d.maxJitterUs+' µs'],['Late ticks',d.lateIntervals],
  ['Max loop work',d.maxWorkUs+' µs'],['Max cmd latency',d.maxCommandLatencyUs+' µs'],['Commands applied',d.commandsProcessed],['Failsafe trips',d.failsafeTrips],
  ['HW stack free',d.hwStackFree+' B'],['Task watchdog',d.wdt?'armed':'off']];
 for(let c=0;c<2;c++){const p=[];if(c===d.netCore)p.push(...net);if(c===d.hwCore)p.push(...hw);kv($('kv'+c),p.length?p:[['Runs','system tasks only']])}
 const tot=d.jitterHist.reduce((a,b)=>a+b,0)||1;
 $('hist').innerHTML=d.jitterHist.map((n,i)=>`<div class="hb"><span>${HL[i]}</span><div><i class="${i<2?'g':i<4?'y':'r'}" style="width:${Math.max(.5,n/tot*100)}%"></i></div><b>${n}</b></div>`).join('');
 for(let i=1;i<=4;i++){const on=d.relays[i-1],e=$('s'+i);e.textContent=on?'ON':'OFF';e.className='pill'+(on?' on':'')}
 kv($('sys'),[['Chip',d.chip],['CPU clock',d.cpuMHz+' MHz'],['Chip temp',d.tempC.toFixed(1)+' °C'],['Free heap',fmtB(d.freeHeap)],['Lowest free heap',fmtB(d.minFreeHeap)],
  ['Largest free block',fmtB(d.maxAllocHeap)],['PSRAM',d.psramSize?fmtB(d.psramSize):'none'],['Last reset',d.resetReason],['Uptime',fmtUp(d.uptimeMs)],
  ['ESP-IDF',d.idf],['Arduino core',d.arduino],['Flash tests',d.flashTests+(d.flashTests?' (last '+(d.lastFlashUs/1000).toFixed(1)+' ms)':'')],['Trace events lost',d.traceLost]]);
 $('conn').textContent=(d.wifiConnected?'Wi-Fi up':'Wi-Fi down')+' · '+d.ip+' · '+d.rssi+' dBm · '+(wsOpen?'live over WebSocket':httpLive?'live over HTTP (WebSocket unavailable)':'polling over HTTP')+' · config v'+d.configVersion;
 if(!wsOpen){setLoad(0,d.coreLoad[0]);setLoad(1,d.coreLoad[1])}
}
function renderTasks(m){
 setLoad(0,m.load[0]);setLoad(1,m.load[1]);
 push(ser.l0,m.load[0]);push(ser.l1,m.load[1]);push(ser.jit,S?S.recentMaxJitterUs:0);
 const g={'0':[],'1':[],'-1':[]};
 for(const t of m.t)(g[t[1]]||g['-1']).push(t);
 for(const k in g){g[k].sort((a,b)=>b[5]-a[5]);
  $(k==='-1'?'tbu':'tb'+k).innerHTML=g[k].map(t=>`<tr class="${MINE.includes(t[0])?'mine':''}"><td>${esc(t[0])}</td><td>${t[2]}</td><td>${STATE[t[3]]||t[3]}</td><td><i class="cpu" style="width:${Math.min(40,t[5]/25)}px"></i>${(t[5]/10).toFixed(1)}%</td><td>${t[4]} B</td></tr>`).join('')||'<tr><td colspan="5">none</td></tr>'}
 $('sampleUs').textContent=m.t.length+' tasks · table read in '+m.sampleUs+' µs';
 sparks();
}
let clkOff=null,clkAt=0;
function addTrace(m){
 // Map device time to page time. The smallest (arrival - device time) seen is the
 // offset with the least network delay; let it relax slowly to follow clock drift.
 const now=performance.now();let base=now;
 if(m.now!==undefined){const dev=m.now/1000,o=now-dev;clkOff=clkOff===null?o:Math.min(clkOff+(now-clkAt)*2e-4,o);clkAt=now;base=dev+clkOff}
 const batch=m.ev.map(a=>({k:a[0],c:a[1],t:base-a[2]/1000,d:a[3],id:a[4],aux:a[5]}));
 evs.push(...batch);
 for(const e of batch){
  if(e.k===1){const q=evs.find(x=>x.k===5&&x.id===e.id);$('journey').textContent=`Command #${e.id}: queued on core ${q?q.c:'?'}, applied on core ${e.c} after ${e.d} µs (mostly waiting for the next 10 ms tick).`}
  else if(e.k===6)log('command #'+e.id+' dropped: queue full');
  else if(e.k===8)log(`CPU hog on core ${e.c} ran ${(e.d/1000).toFixed(0)} ms`);
  else if(e.k===2)log('failsafe: relays switched off');}
 if(++trims%10===0){const cut=(paused?pauseAt:now)-12000;evs=evs.filter(e=>e.t+e.d/1000>cut)}
}
function fit(cv){const r=window.devicePixelRatio||1,W=cv.clientWidth,H=cv.clientHeight;if(cv.width!==Math.round(W*r)||cv.height!==Math.round(H*r)){cv.width=Math.round(W*r);cv.height=Math.round(H*r)}const g=cv.getContext('2d');g.setTransform(r,0,0,r,0,0);g.clearRect(0,0,W,H);return[g,W,H]}
function spark(id,a,max,color,label){
 const[g,W,H]=fit($(id));g.strokeStyle=COL.line;g.beginPath();g.moveTo(0,H-.5);g.lineTo(W,H-.5);g.stroke();
 if(a.length<2)return;const m=max||Math.max(1,...a)*1.2,x=i=>W-(a.length-1-i)*(W/59),y=v=>H-2-Math.min(v,m)/m*(H-8);
 g.beginPath();a.forEach((v,i)=>i?g.lineTo(x(i),y(v)):g.moveTo(x(i),y(v)));g.strokeStyle=color;g.lineWidth=1.5;g.stroke();
 g.lineTo(x(a.length-1),H);g.lineTo(x(0),H);g.closePath();g.globalAlpha=.15;g.fillStyle=color;g.fill();g.globalAlpha=1;
 if(label){g.fillStyle=COL.dim;g.font='10px system-ui';g.fillText(label,2,10)}
}
function sparks(){spark('sp0',ser.l0,100,COL.c0);spark('sp1',ser.l1,100,COL.c1);spark('spj',ser.jit,0,COL.warn,ser.jit.length?'peak '+Math.max(...ser.jit)+' µs':'')}
function draw(){
 requestAnimationFrame(draw);
 const[g,W,H]=fit($('tl'));const L=78,top=20,gap=10,lh=(H-top-8-gap)/2,pw=W-L-6;
 const now=(paused?pauseAt:performance.now())-(wsOpen?150:450),t0=now-win,X=t=>L+(t-t0)/win*pw,Y=c=>top+c*(lh+gap),sc=pw/win;
 for(let c=0;c<2;c++){g.fillStyle='rgba(255,255,255,.035)';g.fillRect(L,Y(c),pw,lh);
  g.fillStyle=c?COL.c1:COL.c0;g.font='600 13px system-ui';g.fillText('Core '+c,6,Y(c)+lh/2-2);
  g.fillStyle=COL.dim;g.font='11px system-ui';g.fillText(role(c),6,Y(c)+lh/2+13)}
 const step=win<=1000?100:win<=2000?250:1000;g.strokeStyle='rgba(147,166,194,.15)';g.fillStyle=COL.dim;g.font='10px system-ui';
 for(let a=0;a<=win;a+=step){const x=X(now-a);g.beginPath();g.moveTo(x,top);g.lineTo(x,H-6);g.stroke();if(a%(step*2)===0||win<=1000)g.fillText(a?'-'+(a/1000)+' s':'now',x-(a?14:22),12)}
 if(!wsOpen&&!httpLive){g.fillStyle=COL.dim;g.font='13px system-ui';g.fillText('Waiting for timeline data...',L+12,top+lh+gap/2+4);return}
 g.save();g.beginPath();g.rect(L,0,pw,H);g.clip();
 const enq=new Map();
 for(const e of evs){if(e.k===5)enq.set(e.id,e);const x=X(e.t),w=Math.max(2,e.d/1000*sc);if(x+w<L||x>L+pw)continue;
  if(e.k===8){g.globalAlpha=.28;g.fillStyle=COL.hog;g.fillRect(x,Y(e.c),w,lh);g.globalAlpha=1;g.fillStyle=COL.hog;g.font='600 11px system-ui';g.fillText('CPU hog',Math.max(x,L)+4,Y(e.c)+12)}
  else if(e.k===9){g.globalAlpha=.28;g.fillStyle=COL.flash;g.fillRect(x,top,w,lh*2+gap);g.globalAlpha=1;g.fillStyle=COL.flash;g.font='600 11px system-ui';g.fillText('flash write',Math.max(x,L)+4,top+lh+gap/2+4)}}
 for(const e of evs){const x=X(e.t);if(x<L-400||x>L+pw)continue;const y=Y(e.c);
  switch(e.k){
  case 0:{g.fillStyle=e.aux<=100?COL.ok:e.aux<=500?COL.warn:COL.bad;const h=e.aux>500?lh-4:lh*.42;g.fillRect(x,y+lh-h-2,1.5,h);break}
  case 3:case 4:case 7:case 10:{const w=Math.max(2,e.d/1000*sc);g.fillStyle=e.k===3?COL.http:e.k===4?COL.ws:COL.push;g.fillRect(x,y+5,w,lh*.36);
   if(w>60&&e.k===3){g.fillStyle='#0b1120';g.font='10px system-ui';g.fillText(ROUTE[e.aux]||'',x+3,y+5+lh*.18+3)}break}
  case 5:g.fillStyle=COL.enq;g.beginPath();g.arc(x,y+lh*.55,4,0,7);g.fill();break;
  case 6:g.strokeStyle=COL.bad;g.lineWidth=2;g.beginPath();g.moveTo(x-4,y+lh*.55-4);g.lineTo(x+4,y+lh*.55+4);g.moveTo(x+4,y+lh*.55-4);g.lineTo(x-4,y+lh*.55+4);g.stroke();g.lineWidth=1;break;
  case 1:{g.fillStyle=COL.apply;g.beginPath();g.arc(x,y+lh*.55,4.5,0,7);g.fill();const q=enq.get(e.id);
   if(q){g.strokeStyle=COL.enq;g.setLineDash([4,3]);g.beginPath();g.moveTo(X(q.t),Y(q.c)+lh*.55);g.lineTo(x,y+lh*.55);g.stroke();g.setLineDash([])}break}
  case 2:g.fillStyle=COL.bad;g.beginPath();g.moveTo(x,y+4);g.lineTo(x+6,y+16);g.lineTo(x-6,y+16);g.fill();break}}
 g.restore();
}
function route(c){
 if(c==='RESET')return['/api/reset',''];if(c==='FLASH')return['/api/flashtest',''];
 if(c.startsWith('HOG:')){const p=c.split(':');return['/api/hog','core='+p[1]+'&ms='+p[2]]}
 if(c.startsWith('ALL:'))return['/api/all','state='+c.slice(4)];
 return['/api/relay','relay='+c[1]+'&state='+c.split(':')[1]]}
async function cmd(c){const[p,q]=route(c);
 try{const r=await fetch(p+(q?'?'+q:''),{method:'POST',headers:{'X-Api-Key':$('tok').value},cache:'no-store'});
  log(c+' -> '+r.status+' '+await r.text()+(r.status===401?'  (enter the API token above)':''))}
 catch(e){log(c+' failed: '+e.message)}}
function hog(){cmd('HOG:'+$('hogCore').value+':'+$('hogMs').value)}
async function poll(p,fn){try{const r=await fetch(p+'?t='+Date.now(),{cache:'no-store'});if(r.ok)fn(await r.json())}catch(e){$('conn').textContent='Device unreachable'}}
async function pollTrace(){
 if(wsOpen){httpLive=false;return}if(traceBusy)return;traceBusy=true;
 try{const r=await fetch('/api/trace?since='+traceSeq+'&t='+Date.now(),{cache:'no-store'});
  if(r.ok){const m=await r.json();traceSeq=m.seq;addTrace(m);httpLive=true}}
 catch(e){httpLive=false}finally{traceBusy=false}}
function connectWS(){
 ws=new WebSocket('ws://'+location.hostname+':81/');
 ws.onopen=()=>{wsOpen=true;log('WebSocket connected')};
 ws.onmessage=e=>{let d;try{d=JSON.parse(e.data)}catch(x){return}
  if(d.type==='status')renderStatus(d);else if(d.type==='tasks')renderTasks(d);else if(d.type==='trace')addTrace(d);else if(d.error)log('WS: '+e.data)};
 ws.onclose=()=>{if(wsOpen)log('WebSocket closed, retrying');wsOpen=false;setTimeout(connectWS,2000)};
 ws.onerror=()=>ws.close();
}
let busy=false;
async function stress(){
 if(busy)return;busy=true;$('stress').disabled=true;
 const total=Number($('num').value);let next=0,ok=0,fail=0;
 const w=async()=>{while(true){const i=next++;if(i>=total)return;
  try{const r=await fetch('/?stress='+i+'&t='+Date.now(),{cache:'no-store'});if(r.ok){await r.text();ok++}else fail++}catch(e){fail++}
  if((ok+fail)%10===0)$('result').textContent=(ok+fail)+'/'+total+' · OK '+ok+' · failed '+fail}};
 await Promise.all([w(),w(),w()]);
 $('result').textContent='Done: '+ok+' OK, '+fail+' failed';log('stress: '+ok+'/'+total+' OK');
 busy=false;$('stress').disabled=false;
}
$('tok').value=sessionStorage.getItem('tok')||'';
$('tok').oninput=()=>sessionStorage.setItem('tok',$('tok').value);
$('win').onchange=e=>win=Number(e.target.value);
$('pause').onclick=()=>{paused=!paused;pauseAt=performance.now();$('pause').textContent=paused?'Resume':'Pause'};
window.addEventListener('resize',sparks);
setInterval(()=>{if(!wsOpen)poll('/api/status',renderStatus)},1000);
setInterval(()=>{if(!wsOpen)poll('/api/tasks',renderTasks)},1000);
setInterval(pollTrace,250);
setInterval(()=>{if($('tok').value)fetch('/api/ping',{method:'POST',headers:{'X-Api-Key':$('tok').value}}).catch(()=>{})},2000);
connectWS();poll('/api/status',renderStatus);poll('/api/tasks',renderTasks);requestAnimationFrame(draw);
</script></body></html>
)HTML";

// -------------------------------------------------------------------
// Core 0 helpers
// -------------------------------------------------------------------
inline void markActivity() { lastActivityMs.store(millis(), std::memory_order_relaxed); }

// Record an event that happened on the network core (networkTask only).
void trace0(uint8_t kind, int64_t tUs, uint32_t durUs, uint32_t id = 0, uint16_t aux = 0,
            uint8_t core = NETWORK_CORE) {
  if (core0TraceCount >= CORE0_TRACE_MAX) { core0TraceLost++; return; }
  core0Trace[core0TraceCount++] = TraceEvt{tUs, durUs, id, aux, kind, core};
}

// Times a block of networkTask work and records it as one span.
struct Span {
  uint8_t kind; uint16_t aux; int64_t t0;
  Span(uint8_t k, uint16_t a = 0) : kind(k), aux(a), t0(esp_timer_get_time()) {}
  ~Span() { trace0(kind, t0, (uint32_t)(esp_timer_get_time() - t0), 0, aux); }
};

enum Route : uint16_t { R_ROOT, R_STATUS, R_RELAY, R_ALL, R_RESET, R_PING, R_TASKS, R_HOG, R_FLASH, R_404, R_TRACE };
enum PushKind : uint16_t { PUSH_STATUS, PUSH_TASKS, PUSH_TRACE };

bool tokenEquals(const char *a, const char *b) {   // constant-time-ish compare
  size_t la = strlen(a), lb = strlen(b);
  size_t n = la > lb ? la : lb;
  uint8_t diff = (la != lb);
  for (size_t i = 0; i < n; ++i) {
    char ca = i < la ? a[i] : 0;
    char cb = i < lb ? b[i] : 0;
    diff |= (uint8_t)(ca ^ cb);
  }
  return diff == 0;
}

bool parseOnOff(const char *s, bool &state) {
  if (!strcasecmp(s, "on") || !strcmp(s, "1") || !strcasecmp(s, "true"))  { state = true;  return true; }
  if (!strcasecmp(s, "off") || !strcmp(s, "0") || !strcasecmp(s, "false")) { state = false; return true; }
  return false;
}

const char *resetReasonStr(esp_reset_reason_t r) {
  switch (r) {
    case ESP_RST_POWERON:   return "power-on";
    case ESP_RST_EXT:       return "external pin";
    case ESP_RST_SW:        return "software restart";
    case ESP_RST_PANIC:     return "crash (panic)";
    case ESP_RST_INT_WDT:   return "interrupt watchdog";
    case ESP_RST_TASK_WDT:  return "task watchdog";
    case ESP_RST_WDT:       return "other watchdog";
    case ESP_RST_DEEPSLEEP: return "deep-sleep wake";
    case ESP_RST_BROWNOUT:  return "brownout";
    case ESP_RST_SDIO:      return "SDIO";
    case ESP_RST_USB:       return "USB (upload / serial reset)";
    case ESP_RST_JTAG:      return "JTAG";
    case ESP_RST_EFUSE:     return "eFuse error";
    case ESP_RST_PWR_GLITCH:return "power glitch";
    case ESP_RST_CPU_LOCKUP:return "CPU lock-up";
    default:                return "unknown";
  }
}

char taskStateChar(eTaskState s) {
  switch (s) {
    case eRunning:   return 'R';
    case eReady:     return 'Y';
    case eBlocked:   return 'B';
    case eSuspended: return 'S';
    case eDeleted:   return 'D';
    default:         return '?';
  }
}

// Single producer: only Core 0 calls this. Never blocks.
bool enqueueCommand(CommandType type, uint8_t relay, bool state, uint32_t &id) {
  Command cmd = {};
  cmd.type = type;
  cmd.relay = relay;
  cmd.state = state;
  cmd.id = nextCommandID++;
  cmd.enqueuedUs = esp_timer_get_time();
  id = cmd.id;
  if (xQueueSend(commandQueue, &cmd, 0) == pdPASS) {
    net.commandsQueued++;
    uint32_t depth = uxQueueMessagesWaiting(commandQueue);
    if (depth > net.queueHighWater) net.queueHighWater = depth;
    trace0(EV_ENQ, cmd.enqueuedUs, 0, cmd.id, type);
    return true;
  }
  net.queueDrops++;
  trace0(EV_DROP, cmd.enqueuedUs, 0, cmd.id, type);
  return false;
}

// -------------------------------------------------------------------
// Per-core CPU load and task table (Core 0, once per second)
//
// FreeRTOS counts how long every task has run. A core's load is the share of
// the last second its IDLE task did NOT run. Reading the table takes ~1.7 ms
// and holds the kernel lock, which both cores share, so it is only done right
// after a control tick (phase alignment). Task switching on this core is
// suspended between that check and the read, so a higher-priority task (Wi-Fi,
// lwIP) cannot delay the read into the next tick. There is no time-out that
// forces a read: measured under HTTP load, forced reads hit ticks again.
// Returns false if skipped (the caller simply tries again a few ms later).
// -------------------------------------------------------------------
bool sampleTasks() {
  configRUN_TIME_COUNTER_TYPE total = 0;
  UBaseType_t n = 0;
  HwSnapshot h;
  vTaskSuspendAll();
  const int64_t t0 = esp_timer_get_time();
  bool inWindow = false;
  if (hwSnap.read(h)) {
    const int64_t sinceWake = t0 - h.lastWakeUs;
    inWindow = h.ticks == 0 || (sinceWake >= 0 && sinceWake <= SAMPLE_WINDOW_US) || sinceWake > SAMPLE_STALL_US;
  }
  if (inWindow) n = uxTaskGetSystemState(taskStat, MAX_TASKS, &total);
  const int64_t t1 = esp_timer_get_time();
  xTaskResumeAll();
  if (!inWindow) return false;
  lastSampleUs = (uint32_t)(t1 - t0);
  trace0(EV_SAMPLE, t0, lastSampleUs, n);
  if (n == 0) return true;   // more tasks than MAX_TASKS

  const bool havePrev = prevTotal != 0;
  const configRUN_TIME_COUNTER_TYPE dt = total - prevTotal;
  prevTotal = total;
  const TaskHandle_t idle[2] = { xTaskGetIdleTaskHandleForCore(0), xTaskGetIdleTaskHandleForCore(1) };

  TaskHandle_t                newHandle[MAX_TASKS];
  configRUN_TIME_COUNTER_TYPE newRun[MAX_TASKS];
  taskRowCount = 0;

  for (UBaseType_t i = 0; i < n; ++i) {
    const TaskStatus_t &t = taskStat[i];
    configRUN_TIME_COUNTER_TYPE delta = 0;
    for (int k = 0; k < prevCount; ++k) {
      if (prevHandle[k] == t.xHandle) { delta = t.ulRunTimeCounter - prevRun[k]; break; }
    }
    newHandle[i] = t.xHandle;
    newRun[i] = t.ulRunTimeCounter;

    uint32_t permille = 0;
    if (havePrev && dt) {
      uint64_t p = (uint64_t)delta * 1000u / (uint64_t)dt;
      permille = p > 1000 ? 1000 : (uint32_t)p;
    }
    for (int c = 0; c < 2; ++c) {
      if (havePrev && t.xHandle == idle[c]) {
        uint32_t idlePct = (permille + 5) / 10;
        coreLoad[c] = (uint8_t)(idlePct >= 100 ? 0 : 100 - idlePct);
      }
    }

    TaskRow &r = taskRows[taskRowCount++];
    size_t j = 0;
    for (const char *s = t.pcTaskName; *s && j < sizeof(r.name) - 1; ++s) {
      char ch = *s;
      r.name[j++] = (ch == '"' || ch == '\\' || (uint8_t)ch < 0x20) ? '_' : ch;
    }
    r.name[j] = '\0';
    r.core = (t.xCoreID == tskNO_AFFINITY) ? -1 : (int8_t)t.xCoreID;
    r.prio = (uint8_t)t.uxCurrentPriority;
    r.state = taskStateChar(t.eCurrentState);
    r.stackFree = t.usStackHighWaterMark;   // bytes on ESP-IDF
    r.cpuPermille = (uint16_t)permille;
  }
  memcpy(prevHandle, newHandle, sizeof(TaskHandle_t) * n);
  memcpy(prevRun, newRun, sizeof(configRUN_TIME_COUNTER_TYPE) * n);
  prevCount = (int)n;

  chipTempC = temperatureRead();
  return true;
}

size_t buildTasks(char *out, size_t cap) {
  JBuf j(out, cap);
  j.add("{\"type\":\"tasks\",\"load\":[%u,%u],\"sampleUs\":%lu,\"t\":[",
        (unsigned)coreLoad[0], (unsigned)coreLoad[1], U(lastSampleUs));
  for (int i = 0; i < taskRowCount; ++i) {
    const TaskRow &r = taskRows[i];
    j.add("%s[\"%s\",%d,%u,\"%c\",%lu,%u]", i ? "," : "", r.name, (int)r.core,
          (unsigned)r.prio, r.state, U(r.stackFree), (unsigned)r.cpuPermille);
  }
  j.add("]}");
  if (j.ovf) { JBuf e(out, cap); e.add("{\"type\":\"tasks\",\"error\":\"too many tasks\"}"); return e.len; }
  return j.len;
}

size_t buildStatus(char *out, size_t cap, const HwSnapshot &h) {
  JBuf j(out, cap);
  int wsClients = serversRunning ? webSocket.connectedClients() : 0;
  j.add("{\"type\":\"status\",\"wifiConnected\":%s,\"ip\":\"%s\",\"rssi\":%d,\"uptimeMs\":%lu,",
        wifiUp ? "true" : "false", WiFi.localIP().toString().c_str(), (int)WiFi.RSSI(), U(millis()));
  j.add("\"netCore\":%d,\"hwCore\":%u,\"coreLoad\":[%u,%u],\"wdt\":%s,",
        NETWORK_CORE, (unsigned)h.core, (unsigned)coreLoad[0], (unsigned)coreLoad[1],
        h.wdtArmed ? "true" : "false");
  j.add("\"freeHeap\":%lu,\"minFreeHeap\":%lu,\"maxAllocHeap\":%lu,\"psramSize\":%lu,",
        U(ESP.getFreeHeap()), U(ESP.getMinFreeHeap()), U(ESP.getMaxAllocHeap()), U(ESP.getPsramSize()));
  j.add("\"chip\":\"%s rev %u\",\"cpuMHz\":%lu,\"tempC\":%.1f,\"resetReason\":\"%s\",\"idf\":\"%s\",\"arduino\":\"%d.%d.%d\",",
        ESP.getChipModel(), (unsigned)ESP.getChipRevision(), U(ESP.getCpuFreqMHz()), (double)chipTempC,
        resetReasonStr(bootReason), esp_get_idf_version(),
        ESP_ARDUINO_VERSION_MAJOR, ESP_ARDUINO_VERSION_MINOR, ESP_ARDUINO_VERSION_PATCH);
  j.add("\"networkExecutions\":%lu,\"queueDepth\":%lu,\"queueLength\":%u,\"queueHighWater\":%lu,",
        U(net.executions), U(uxQueueMessagesWaiting(commandQueue)), (unsigned)QUEUE_LENGTH, U(net.queueHighWater));
  j.add("\"rootRequests\":%lu,\"statusRequests\":%lu,\"commandsQueued\":%lu,\"commandsRejected\":%lu,"
        "\"queueDrops\":%lu,\"authFailures\":%lu,",
        U(net.rootRequests), U(net.statusRequests), U(net.commandsQueued), U(net.commandsRejected),
        U(net.queueDrops), U(net.authFailures));
  j.add("\"netLoopLastUs\":%lu,\"netLoopMaxUs\":%lu,\"hwStackFree\":%lu,\"netStackFree\":%lu,\"wsClients\":%d,",
        U(net.loopLastUs), U(net.loopMaxUs),
        U(hardwareTaskHandle ? uxTaskGetStackHighWaterMark(hardwareTaskHandle) : 0),
        U(networkTaskHandle ? uxTaskGetStackHighWaterMark(networkTaskHandle) : 0), wsClients);
  j.add("\"configVersion\":%lu,\"hardwareTicks\":%lu,\"commandsProcessed\":%lu,\"hardwareRejected\":%lu,"
        "\"totalOperations\":%lu,",
        U(h.version), U(h.ticks), U(h.processed), U(h.rejected), U(h.totalOperations));
  j.add("\"lastIntervalUs\":%lu,\"lastJitterUs\":%lu,\"maxJitterUs\":%lu,\"recentMaxJitterUs\":%lu,"
        "\"lateIntervals\":%lu,",
        U(h.lastIntervalUs), U(h.lastJitterUs), U(h.maxJitterUs), U(h.recentMaxJitterUs), U(h.lateIntervals));
  j.add("\"lastCommandLatencyUs\":%lu,\"maxCommandLatencyUs\":%lu,\"maxWorkUs\":%lu,\"recentMaxWorkUs\":%lu,"
        "\"failsafeTrips\":%lu,\"traceLost\":%lu,",
        U(h.lastLatencyUs), U(h.maxLatencyUs), U(h.maxWorkUs), U(h.recentMaxWorkUs),
        U(h.failsafeTrips), U(h.traceLost + core0TraceLost));
  j.add("\"hogActive\":%s,\"flashTests\":%lu,\"lastFlashUs\":%lu,",
        hogBusy.load() ? "true" : "false", U(flashTests), U(lastFlashUs));
  j.add("\"jitterHist\":[%lu,%lu,%lu,%lu,%lu,%lu],\"relays\":[%s,%s,%s,%s]}",
        U(h.jitterHist[0]), U(h.jitterHist[1]), U(h.jitterHist[2]),
        U(h.jitterHist[3]), U(h.jitterHist[4]), U(h.jitterHist[5]),
        h.relays[0] ? "true" : "false", h.relays[1] ? "true" : "false",
        h.relays[2] ? "true" : "false", h.relays[3] ? "true" : "false");
  if (j.ovf) { JBuf e(out, cap); e.add("{\"error\":\"status too large\"}"); return e.len; }
  return j.len;
}

// One event as [kind,core,ageUs,durUs,id,aux]. Ages (not absolute times) keep
// the browser independent of the device clock.
void appendEvent(JBuf &j, const TraceEvt &e, int64_t now, bool first) {
  int64_t age = now - e.tUs;
  if (age < 0) age = 0;
  j.add("%s[%u,%u,%lu,%lu,%lu,%u]", first ? "" : ",", (unsigned)e.kind, (unsigned)e.core,
        U(age > 0xFFFFFFFFLL ? 0xFFFFFFFFLL : age), U(e.durUs), U(e.id), (unsigned)e.aux);
}

// Send everything recorded since the last flush as one compact batch:
// {"type":"trace","ev":[[kind,core,ageUs,durUs,id,aux],...]}
// Every event is also kept in traceHist for the HTTP fallback.
void flushTrace() {
  const bool send = serversRunning && webSocket.connectedClients() > 0;
  const int64_t now = esp_timer_get_time();
  JBuf j(traceBuf, sizeof traceBuf);
  bool first = true;
  uint32_t overflow = 0;
  auto emit = [&](const TraceEvt &e) {
    traceHist[traceHistSeq % TRACE_HISTORY] = e;
    traceHistSeq++;
    if (!send) return;
    if (j.cap - j.len < 64) { overflow++; return; }
    appendEvent(j, e, now, first);
    first = false;
  };
  if (send) j.add("{\"type\":\"trace\",\"now\":%lld,\"ev\":[", (long long)now);
  for (uint16_t i = 0; i < core0TraceCount; ++i) emit(core0Trace[i]);
  core0TraceCount = 0;
  TraceEvt e;
  while (traceRing.pop(e)) emit(e);
  core0TraceLost += overflow;
  if (!send || first) return;     // nothing to send
  j.add("]}");
  if (j.ovf) return;
  webSocket.broadcastTXT(traceBuf, j.len);
  trace0(EV_PUSH, now, (uint32_t)(esp_timer_get_time() - now), 0, PUSH_TRACE);
}

// -------------------------------------------------------------------
// Experiments (Core 0 starts them)
// -------------------------------------------------------------------
void hogTask(void *) {
  vTaskDelay(pdMS_TO_TICKS(40));          // let the HTTP/WS reply leave first
  const int64_t t0 = esp_timer_get_time();
  const int64_t until = t0 + (int64_t)hogMs * 1000;
  hogStartUs = t0;
  volatile uint32_t spins = 0;
  while (esp_timer_get_time() < until) spins = spins + 1;   // busy, never yields
  hogEndUs = esp_timer_get_time();
  hogDone.store(true, std::memory_order_release);
  vTaskDelete(nullptr);
}

bool startHog(uint8_t core, uint32_t ms) {
  bool expected = false;
  if (!hogBusy.compare_exchange_strong(expected, true)) return false;
  hogCore = core;
  hogMs = ms;
  hogDone.store(false, std::memory_order_relaxed);
  if (xTaskCreatePinnedToCore(hogTask, "CpuHog", 3072, nullptr, HOG_PRIORITY, nullptr, core) != pdPASS) {
    hogBusy.store(false);
    return false;
  }
  return true;
}

// While SPI flash is written the cache is disabled and the OTHER core is parked
// in a spin loop, so this pauses the control loop even though it is on Core 1.
uint32_t runFlashTest() {
  static uint8_t blob[1024];
  const int64_t t0 = esp_timer_get_time();
  if (prefs.begin("demo", false)) {
    for (int i = 0; i < 4; ++i) {
      uint32_t r = esp_random();
      for (size_t k = 0; k < sizeof blob; ++k) blob[k] = (uint8_t)(r + k * 31u);
      prefs.putBytes("blob", blob, sizeof blob);
    }
    prefs.end();
  }
  const uint32_t dur = (uint32_t)(esp_timer_get_time() - t0);
  flashTests++;
  lastFlashUs = dur;
  trace0(EV_FLASH, t0, dur, flashTests);
  return dur;
}

// -------------------------------------------------------------------
// HTTP handlers (Core 0)
// -------------------------------------------------------------------
void sendJson(int status, const char *body) {
  httpServer.sendHeader("Cache-Control", "no-store, max-age=0");
  httpServer.send(status, "application/json", body);
}

bool requireAuth() {
#if REQUIRE_AUTH
  String key = httpServer.header("X-Api-Key");
  if (!tokenEquals(key.c_str(), API_TOKEN)) {
    net.authFailures++;
    sendJson(401, "{\"error\":\"unauthorized\"}");
    return false;
  }
#endif
  markActivity();
  return true;
}

void replyQueued(bool ok, uint32_t id) {
  char b[96];
  if (ok) { snprintf(b, sizeof b, "{\"queued\":true,\"id\":%lu,\"core\":%d}", U(id), xPortGetCoreID()); sendJson(202, b); }
  else    { sendJson(503, "{\"queued\":false,\"error\":\"queue full\"}"); }
}

void handleRoot() {
  Span span(EV_HTTP, R_ROOT);
  net.rootRequests++;
  httpServer.sendHeader("Cache-Control", "no-store, max-age=0");
  httpServer.send_P(200, "text/html; charset=utf-8", dashboardHTML);
}

void handleStatus() {
  Span span(EV_HTTP, R_STATUS);
  net.statusRequests++;
  HwSnapshot h;
  if (!hwSnap.read(h)) { sendJson(503, "{\"error\":\"snapshot busy\"}"); return; }
  buildStatus(jsonBuf, sizeof jsonBuf, h);
  sendJson(200, jsonBuf);
}

void handleTasks() {
  Span span(EV_HTTP, R_TASKS);
  buildTasks(tasksBuf, sizeof tasksBuf);
  sendJson(200, tasksBuf);
}

// HTTP fallback for the live timeline: GET /api/trace?since=<seq>
// returns the events recorded after <seq> plus the new "seq" to ask for next.
void handleTrace() {
  Span span(EV_HTTP, R_TRACE);
  const uint32_t oldest = traceHistSeq > TRACE_HISTORY ? traceHistSeq - TRACE_HISTORY : 0;
  uint32_t since = httpServer.hasArg("since") ? (uint32_t)strtoul(httpServer.arg("since").c_str(), nullptr, 10) : 0;
  if (since == 0 || since < oldest || since > traceHistSeq)          // first call, too old, or device rebooted
    since = traceHistSeq > 300 ? traceHistSeq - 300 : 0;
  if (since < oldest) since = oldest;
  const int64_t now = esp_timer_get_time();
  JBuf j(traceBuf, sizeof traceBuf);
  j.add("{\"type\":\"trace\",\"now\":%lld,\"ev\":[", (long long)now);
  uint32_t s = since;
  for (; s < traceHistSeq; ++s) {
    if (j.cap - j.len < 80) break;                                   // client asks again for the rest
    appendEvent(j, traceHist[s % TRACE_HISTORY], now, s == since);
  }
  j.add("],\"seq\":%lu}", U(s));
  sendJson(200, j.ovf ? "{\"type\":\"trace\",\"ev\":[],\"seq\":0}" : traceBuf);
}

void handleRelay() {
  Span span(EV_HTTP, R_RELAY);
  if (!requireAuth()) return;
  if (!httpServer.hasArg("relay") || !httpServer.hasArg("state")) {
    net.commandsRejected++;
    sendJson(400, "{\"error\":\"expected relay=1..4 and state=on/off\"}");
    return;
  }
  String r = httpServer.arg("relay");
  bool state = false;
  if (r.length() != 1 || r[0] < '1' || r[0] > '4' || !parseOnOff(httpServer.arg("state").c_str(), state)) {
    net.commandsRejected++;
    sendJson(400, "{\"error\":\"invalid relay or state\"}");
    return;
  }
  uint32_t id;
  replyQueued(enqueueCommand(RELAY_CONTROL, r[0] - '1', state, id), id);
}

void handleAll() {
  Span span(EV_HTTP, R_ALL);
  if (!requireAuth()) return;
  bool state = false;
  if (!httpServer.hasArg("state") || !parseOnOff(httpServer.arg("state").c_str(), state)) {
    net.commandsRejected++;
    sendJson(400, "{\"error\":\"expected state=on/off\"}");
    return;
  }
  uint32_t id;
  replyQueued(enqueueCommand(state ? ALL_RELAYS_ON : ALL_RELAYS_OFF, 0, state, id), id);
}

void handleReset() {
  Span span(EV_HTTP, R_RESET);
  if (!requireAuth()) return;
  uint32_t id;
  replyQueued(enqueueCommand(RESET_STATS, 0, false, id), id);
}

void handlePing() {
  Span span(EV_HTTP, R_PING);
  if (!requireAuth()) return;
  sendJson(200, "{\"ok\":true}");
}

void handleHog() {
  Span span(EV_HTTP, R_HOG);
  if (!requireAuth()) return;
  long core = httpServer.hasArg("core") ? httpServer.arg("core").toInt() : 0;
  long ms   = httpServer.hasArg("ms") ? httpServer.arg("ms").toInt() : 500;
  if ((core != 0 && core != 1) || ms < (long)HOG_MIN_MS || ms > (long)HOG_MAX_MS) {
    net.commandsRejected++;
    sendJson(400, "{\"error\":\"expected core=0|1 and ms=50..2000\"}");
    return;
  }
  if (!startHog((uint8_t)core, (uint32_t)ms)) { sendJson(409, "{\"error\":\"a hog is already running\"}"); return; }
  char b[80];
  snprintf(b, sizeof b, "{\"ok\":true,\"core\":%ld,\"ms\":%ld}", core, ms);
  sendJson(202, b);
}

void handleFlashTest() {
  Span span(EV_HTTP, R_FLASH);
  if (!requireAuth()) return;
  uint32_t us = runFlashTest();
  char b[80];
  snprintf(b, sizeof b, "{\"ok\":true,\"us\":%lu,\"core\":%d}", U(us), xPortGetCoreID());
  sendJson(200, b);
}

// -------------------------------------------------------------------
// WebSocket (Core 0)
// -------------------------------------------------------------------
void wsReply(uint8_t c, const char *s) { webSocket.sendTXT(c, s); }

void webSocketEvent(uint8_t client, WStype_t type, uint8_t *payload, size_t length) {
  if (client >= MAX_WS_CLIENTS) return;

  if (type == WStype_DISCONNECTED) { wsAuthed[client] = false; return; }
  if (type == WStype_CONNECTED) {
    wsAuthed[client] = !REQUIRE_AUTH;
    wsReply(client, REQUIRE_AUTH ? "{\"event\":\"connected\",\"authRequired\":true}"
                                 : "{\"event\":\"connected\",\"authRequired\":false}");
    return;
  }
  if (type != WStype_TEXT) return;
  Span span(EV_WS);

  if (length == 0 || length > 48) {
    net.commandsRejected++;
    wsReply(client, "{\"error\":\"invalid message length\"}");
    return;
  }
  char msg[49];
  memcpy(msg, payload, length);
  msg[length] = '\0';
  // trim
  char *s = msg;
  while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n') ++s;
  size_t n = strlen(s);
  while (n && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '\r' || s[n - 1] == '\n')) s[--n] = '\0';

  // AUTH is case-sensitive on the token
  if (strncasecmp(s, "AUTH:", 5) == 0) {
    bool ok = tokenEquals(s + 5, API_TOKEN);
    wsAuthed[client] = ok;
    if (!ok) net.authFailures++;
    wsReply(client, ok ? "{\"event\":\"auth\",\"ok\":true}" : "{\"event\":\"auth\",\"ok\":false}");
    return;
  }
  for (char *p = s; *p; ++p) *p = (char)toupper((unsigned char)*p);

  if (!strcmp(s, "STATUS")) {
    HwSnapshot h;
    if (hwSnap.read(h)) { buildStatus(jsonBuf, sizeof jsonBuf, h); wsReply(client, jsonBuf); }
    else wsReply(client, "{\"error\":\"snapshot busy\"}");
    return;
  }
  if (!wsAuthed[client]) {
    net.authFailures++;
    wsReply(client, "{\"error\":\"send AUTH:<token> first\"}");
    return;
  }
  markActivity();
  if (!strcmp(s, "PING")) { wsReply(client, "{\"event\":\"pong\"}"); return; }

  char b[96];
  if (!strcmp(s, "FLASH")) {
    snprintf(b, sizeof b, "{\"event\":\"flash\",\"us\":%lu}", U(runFlashTest()));
    wsReply(client, b);
    return;
  }
  unsigned hc = 0, hms = 0;
  if (sscanf(s, "HOG:%u:%u", &hc, &hms) == 2) {
    if (hc > 1 || hms < HOG_MIN_MS || hms > HOG_MAX_MS) { wsReply(client, "{\"error\":\"HOG:<0|1>:<50..2000>\"}"); return; }
    wsReply(client, startHog((uint8_t)hc, hms) ? "{\"event\":\"hog\",\"ok\":true}" : "{\"error\":\"a hog is already running\"}");
    return;
  }

  CommandType type2 = RELAY_CONTROL;
  uint8_t relay = 0;
  bool state = false, valid = false;
  if (!strcmp(s, "RESET")) {
    type2 = RESET_STATS; valid = true;
  } else if (strncmp(s, "ALL:", 4) == 0) {
    valid = parseOnOff(s + 4, state);
    type2 = state ? ALL_RELAYS_ON : ALL_RELAYS_OFF;
  } else if (strlen(s) >= 4 && s[0] == 'R' && s[1] >= '1' && s[1] <= '4' && s[2] == ':') {
    relay = s[1] - '1';
    valid = parseOnOff(s + 3, state);
  }
  if (!valid) {
    net.commandsRejected++;
    wsReply(client, "{\"error\":\"use AUTH:token, R1:ON, ALL:OFF, RESET, PING, STATUS, HOG:0:500, FLASH\"}");
    return;
  }
  uint32_t id;
  bool queued = enqueueCommand(type2, relay, state, id);
  if (queued) snprintf(b, sizeof b, "{\"queued\":true,\"id\":%lu,\"core\":%d}", U(id), xPortGetCoreID());
  else snprintf(b, sizeof b, "{\"queued\":false,\"error\":\"queue full\"}");
  wsReply(client, b);
}

// -------------------------------------------------------------------
// Wi-Fi / servers (Core 0)
// -------------------------------------------------------------------
void startServers() {
  httpServer.on("/", HTTP_GET, handleRoot);
  httpServer.on("/api/status", HTTP_GET, handleStatus);
  httpServer.on("/api/tasks", HTTP_GET, handleTasks);
  httpServer.on("/api/trace", HTTP_GET, handleTrace);
  httpServer.on("/api/relay", HTTP_POST, handleRelay);
  httpServer.on("/api/all", HTTP_POST, handleAll);
  httpServer.on("/api/reset", HTTP_POST, handleReset);
  httpServer.on("/api/ping", HTTP_POST, handlePing);
  httpServer.on("/api/hog", HTTP_POST, handleHog);
  httpServer.on("/api/flashtest", HTTP_POST, handleFlashTest);
  httpServer.onNotFound([]() { Span span(EV_HTTP, R_404); sendJson(404, "{\"error\":\"not found or wrong method\"}"); });
  static const char *headerKeys[] = {"X-Api-Key"};
  httpServer.collectHeaders(headerKeys, 1);
  httpServer.begin();

  webSocket.onEvent(webSocketEvent);
  webSocket.enableHeartbeat(15000, 3000, 2);   // drop dead clients
  webSocket.begin();

  if (MDNS.begin(HOSTNAME)) MDNS.addService("http", "tcp", 80);
  serversRunning = true;
  Serial.printf("[NETWORK] http://%s/  ws://%s:81/  (%s.local)\n",
                WiFi.localIP().toString().c_str(), WiFi.localIP().toString().c_str(), HOSTNAME);
}

void connectToWiFi() {
  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setHostname(HOSTNAME);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  lastWifiRetry = millis();
  Serial.printf("[NETWORK] Connecting to SSID: %s\n", WIFI_SSID);
}

void monitorWiFi() {
  bool connected = (WiFi.status() == WL_CONNECTED);
  if (connected && !wifiUp) {
    wifiUp = true;
    Serial.printf("[WIFI] Connected; IP=%s RSSI=%d dBm\n", WiFi.localIP().toString().c_str(), WiFi.RSSI());
    if (!serversRunning) startServers();      // started once, never torn down
  } else if (!connected && wifiUp) {
    wifiUp = false;
    Serial.println("[WIFI] Disconnected; control loop keeps running on its own core");
    lastWifiRetry = millis();
  }
  // Auto-reconnect handles most drops; this is only a slow backstop.
  if (!connected && millis() - lastWifiRetry >= WIFI_RETRY_MS) {
    lastWifiRetry = millis();
    WiFi.reconnect();
  }
}

void networkTask(void *) {
  connectToWiFi();
  uint32_t lastWifiCheck = 0, lastCheck = 0, lastFullPush = 0, lastTrace = 0, lastSample = 0;
  uint32_t lastPushedVersion = 0xFFFFFFFFu;

  while (true) {
    int64_t t0 = esp_timer_get_time();
    net.executions++;
    uint32_t now = millis();

    if (now - lastWifiCheck >= WIFI_MONITOR_MS) { lastWifiCheck = now; monitorWiFi(); }

    if (hogDone.load(std::memory_order_acquire)) {
      hogDone.store(false, std::memory_order_relaxed);
      trace0(EV_HOG, hogStartUs, (uint32_t)(hogEndUs - hogStartUs), 0, 0, hogCore);
      hogBusy.store(false);
    }

    if (now - lastSample >= TASK_SAMPLE_MS && sampleTasks()) {
      lastSample = now;
      if (serversRunning && webSocket.connectedClients() > 0) {
        Span span(EV_PUSH, PUSH_TASKS);
        size_t len = buildTasks(tasksBuf, sizeof tasksBuf);
        webSocket.broadcastTXT(tasksBuf, len);
      }
    }

    if (serversRunning) {
      httpServer.handleClient();
      webSocket.loop();

      if (now - lastCheck >= STATUS_CHECK_MS) {
        lastCheck = now;
        if (webSocket.connectedClients() > 0) {
          HwSnapshot h;
          if (hwSnap.read(h) && (h.version != lastPushedVersion || now - lastFullPush >= STATUS_HEARTBEAT_MS)) {
            Span span(EV_PUSH, PUSH_STATUS);
            lastPushedVersion = h.version;
            lastFullPush = now;
            size_t len = buildStatus(jsonBuf, sizeof jsonBuf, h);
            webSocket.broadcastTXT(jsonBuf, len);
          }
        }
      }
    }

    // Always drain the trace ring so Core 1 never sees it full for long.
    if (now - lastTrace >= TRACE_FLUSH_MS) { lastTrace = now; flushTrace(); }

    uint32_t busy = (uint32_t)(esp_timer_get_time() - t0);
    net.loopLastUs = busy;
    if (busy > net.loopMaxUs) net.loopMaxUs = busy;

    vTaskDelay(pdMS_TO_TICKS(NETWORK_POLL_MS));
  }
}

// -------------------------------------------------------------------
// Hardware task (HARDWARE_CORE): owns all hardware state, no HTTP/WS/Serial.
// -------------------------------------------------------------------
inline void applyRelayOutput(uint8_t i, bool on) {
#if ENABLE_PHYSICAL_RELAYS
  digitalWrite(RELAY_PINS[i], (on == (RELAY_ACTIVE_HIGH != 0)) ? HIGH : LOW);
#else
  (void)i; (void)on;
#endif
}

void setAllRelays(bool on) {
  for (uint8_t i = 0; i < TOTAL_RELAYS; ++i) { relayState[i] = on; applyRelayOutput(i, on); }
}

void hardwareTask(void *) {
  const TickType_t periodTicks = pdMS_TO_TICKS(HARDWARE_PERIOD_MS) ? pdMS_TO_TICKS(HARDWARE_PERIOD_MS) : 1;
  const uint32_t expectedUs = (uint32_t)(((uint64_t)periodTicks * 1000000ULL) / configTICK_RATE_HZ);
  const uint8_t myCore = (uint8_t)xPortGetCoreID();
  HwSnapshot hw = {};
  hw.core = myCore;
  // If this loop ever stops for 5 s, the task watchdog resets the chip
  // (relays start OFF after reset). The reset reason shows on the dashboard.
  hw.wdtArmed = (esp_task_wdt_add(nullptr) == ESP_OK);

  TickType_t lastWake = xTaskGetTickCount();
  int64_t lastTickUs = esp_timer_get_time();
  bool timingLevel = false;
  bool failsafeLatched = false;
  uint32_t winJitter = 0, prevWinJitter = 0, winWork = 0, prevWinWork = 0;
  uint8_t  winTicks = 0;
#if ENABLE_HEARTBEAT_LED && defined(RGB_BUILTIN)
  bool ledOn = false;
#endif

  while (true) {
    vTaskDelayUntil(&lastWake, periodTicks);
    int64_t wakeUs = esp_timer_get_time();
    uint32_t elapsed = (uint32_t)(wakeUs - lastTickUs);
    lastTickUs = wakeUs;

    uint32_t jitter = elapsed > expectedUs ? elapsed - expectedUs : expectedUs - elapsed;
    hw.lastWakeUs = wakeUs;
    hw.lastIntervalUs = elapsed;
    hw.lastJitterUs = jitter;
    if (jitter > hw.maxJitterUs) hw.maxJitterUs = jitter;
    if (elapsed > expectedUs + expectedUs / 2) hw.lateIntervals++;
    uint8_t bucket = 0;
    while (bucket < HIST_BUCKETS - 1 && jitter > HIST_EDGES_US[bucket]) ++bucket;
    hw.jitterHist[bucket]++;
    hw.ticks++;

    int64_t workStart = esp_timer_get_time();
    if (hw.wdtArmed) esp_task_wdt_reset();

#if ENABLE_TIMING_GPIO
    timingLevel = !timingLevel;
    digitalWrite(TIMING_PIN, timingLevel ? HIGH : LOW);
#else
    (void)timingLevel;
#endif

    Command cmd;
    uint8_t handled = 0;
    while (handled < MAX_COMMANDS_PER_TICK && xQueueReceive(commandQueue, &cmd, 0) == pdPASS) {
      ++handled;
      hw.processed++;
      bool changed = false;
      switch (cmd.type) {
        case RELAY_CONTROL:
          if (cmd.relay < TOTAL_RELAYS) {
            relayState[cmd.relay] = cmd.state;
            applyRelayOutput(cmd.relay, cmd.state);
            hw.totalOperations++;
            changed = true;
          } else hw.rejected++;
          break;
        case ALL_RELAYS_ON:  setAllRelays(true);  hw.totalOperations++; changed = true; break;
        case ALL_RELAYS_OFF: setAllRelays(false); hw.totalOperations++; changed = true; break;
        case RESET_STATS:
          hw.maxJitterUs = 0; hw.lateIntervals = 0; hw.maxLatencyUs = 0; hw.maxWorkUs = 0;
          memset(hw.jitterHist, 0, sizeof hw.jitterHist);
          break;
        default: hw.rejected++; break;
      }
      if (changed) hw.version++;
      int64_t appliedUs = esp_timer_get_time();
      hw.lastLatencyUs = (uint32_t)(appliedUs - cmd.enqueuedUs);
      if (hw.lastLatencyUs > hw.maxLatencyUs) hw.maxLatencyUs = hw.lastLatencyUs;
      if (!traceRing.push(TraceEvt{appliedUs, hw.lastLatencyUs, cmd.id, cmd.type, EV_APPLY, myCore})) hw.traceLost++;
    }

    // Optional failsafe: no authenticated activity -> relays OFF.
    if (FAILSAFE_TIMEOUT_MS > 0) {
      uint32_t idle = millis() - lastActivityMs.load(std::memory_order_relaxed);
      if (idle > FAILSAFE_TIMEOUT_MS) {
        if (!failsafeLatched) {
          failsafeLatched = true;
          bool any = false;
          for (uint8_t i = 0; i < TOTAL_RELAYS; ++i) any |= relayState[i];
          if (any) {
            setAllRelays(false); hw.failsafeTrips++; hw.version++;
            if (!traceRing.push(TraceEvt{esp_timer_get_time(), 0, hw.failsafeTrips, 0, EV_FAILSAFE, myCore})) hw.traceLost++;
          }
        }
      } else failsafeLatched = false;
    }

#if ENABLE_HEARTBEAT_LED && defined(RGB_BUILTIN)
    if (hw.ticks % 50 == 0) { ledOn = !ledOn; rgbLedWrite(RGB_BUILTIN, 0, ledOn ? 24 : 0, ledOn ? 8 : 0); }
#endif

    uint32_t work = (uint32_t)(esp_timer_get_time() - workStart);
    if (work > hw.maxWorkUs) hw.maxWorkUs = work;

    // Rolling "recent" maxima covering the last 1-2 seconds.
    if (jitter > winJitter) winJitter = jitter;
    if (work > winWork) winWork = work;
    if (++winTicks >= 100) { prevWinJitter = winJitter; prevWinWork = winWork; winJitter = winWork = 0; winTicks = 0; }
    hw.recentMaxJitterUs = winJitter > prevWinJitter ? winJitter : prevWinJitter;
    hw.recentMaxWorkUs   = winWork > prevWinWork ? winWork : prevWinWork;

    uint16_t jitter16 = jitter > 0xFFFF ? 0xFFFF : (uint16_t)jitter;
    if (!traceRing.push(TraceEvt{wakeUs, work, hw.ticks, jitter16, EV_TICK, myCore})) hw.traceLost++;

    for (uint8_t i = 0; i < TOTAL_RELAYS; ++i) hw.relays[i] = relayState[i];
    hwSnap.publish(hw);          // lock-free; never blocks the control loop
  }
}

// -------------------------------------------------------------------
void setup() {
#if ENABLE_PHYSICAL_RELAYS
  // First thing: drive every relay pin to its inactive level.
  for (uint8_t i = 0; i < TOTAL_RELAYS; ++i) {
    pinMode(RELAY_PINS[i], OUTPUT);
    digitalWrite(RELAY_PINS[i], RELAY_ACTIVE_HIGH ? LOW : HIGH);
  }
#endif
#if ENABLE_TIMING_GPIO
  pinMode(TIMING_PIN, OUTPUT);
  digitalWrite(TIMING_PIN, LOW);
#endif

  bootReason = esp_reset_reason();
  Serial.begin(115200);
  delay(600);
  Serial.println("\n=== ESP32-S3 MULTICORE WEB HARDWARE CONTROLLER v3 ===");
  Serial.printf("[BOOT] Reset reason: %s\n", resetReasonStr(bootReason));
#if REQUIRE_AUTH
  if (!strcmp(API_TOKEN, "change-me"))
    Serial.println("[WARN] Default API token in use. Set API_TOKEN in secrets.h");
#endif
  if (!strcmp(WIFI_SSID, "your-ssid"))
    Serial.println("[WARN] No Wi-Fi credentials. Create secrets.h with WIFI_SSID / WIFI_PASSWORD");

  markActivity();
  chipTempC = temperatureRead();
  commandQueue = xQueueCreate(QUEUE_LENGTH, sizeof(Command));
  if (!commandQueue) {
    Serial.println("FATAL: cannot allocate command queue");
    while (true) delay(1000);
  }

  BaseType_t r0 = xTaskCreatePinnedToCore(networkTask,  "NetworkTask",  10240, nullptr, NETWORK_PRIORITY,
                                          &networkTaskHandle, NETWORK_CORE);
  BaseType_t r1 = xTaskCreatePinnedToCore(hardwareTask, "HardwareTask", 4096, nullptr, HARDWARE_PRIORITY,
                                          &hardwareTaskHandle, HARDWARE_CORE);
  if (r0 != pdPASS || r1 != pdPASS) {
    Serial.println("FATAL: task creation failed");
    while (true) delay(1000);
  }
  Serial.printf("[OK] Core %d: NetworkTask prio %u | Core %d: HardwareTask prio %u (10 ms)\n",
                NETWORK_CORE, (unsigned)NETWORK_PRIORITY, HARDWARE_CORE, (unsigned)HARDWARE_PRIORITY);
  if (HARDWARE_CORE == NETWORK_CORE)
    Serial.println("[MODE] Comparison mode: control loop shares Core 0 with Wi-Fi");
  Serial.println(ENABLE_PHYSICAL_RELAYS ? "[MODE] Physical relay GPIO enabled" : "[MODE] Virtual relays");
  if (FAILSAFE_TIMEOUT_MS) Serial.printf("[MODE] Failsafe: relays OFF after %lu ms idle\n", U(FAILSAFE_TIMEOUT_MS));
}

void loop() {
  // The Arduino loop task has nothing to do; remove it so Core 1 only shows
  // the control loop and the idle task in the task table.
  vTaskDelete(nullptr);
}
