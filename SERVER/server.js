
console.log("🔥 SERVER START");

require("dotenv").config();

const express = require("express");
const http = require("http");
const mongoose = require("mongoose");
const { Server } = require("socket.io");
const mqtt = require("mqtt");

// ===== INIT =====
const app = express();
app.use(express.json());
/////// AI-test 
const cors = require('cors')
app.use(cors())

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

// ===== CONNECT MONGODB =====
mongoose.connect("mongodb://Hoaidat:hoaidat2004@ac-ohpuiiy-shard-00-00.iarmzxb.mongodb.net:27017,ac-ohpuiiy-shard-00-01.iarmzxb.mongodb.net:27017,ac-ohpuiiy-shard-00-02.iarmzxb.mongodb.net:27017/iot?ssl=true&replicaSet=atlas-qh7yxn-shard-0&authSource=admin&appName=Cluster0")
  .then(() => console.log("✅ MongoDB connected"))
  .catch(err => console.log("❌ DB error:", err));

// ===== SCHEMAS =====

// Schema cho HR và SpO2 (tần số thấp hơn)
const VitalSchema = new mongoose.Schema({
  hr: Number,       // Heart Rate (bpm)
  spo2: Number,     // SpO2 (%)
  time: { type: Date, default: Date.now }
});

// Schema cho Fall Detection (tần số cao hơn)
const FallSchema = new mongoose.Schema({
  fall_detected: { type: Boolean, required: true },
  time: { type: Date, default: Date.now }
});

const Vital = mongoose.model("Vital", VitalSchema);
const Fall  = mongoose.model("Fall",  FallSchema);

// ===== MQTT CONNECT =====
const client = mqtt.connect(
  "wss://a07a6fff42de4e72968999f448c09e7c.s1.eu.hivemq.cloud:8884/mqtt",
  {
    username: "hoaidatne",
    password: "Hoaidat2004@"
  }
);

client.on("connect", () => {
  console.log("✅ MQTT connected");

  // Topic cho HR + SpO2
  client.subscribe("esp32/vital", (err) => {
    if (err) console.log("❌ Subscribe vital error:", err.message);
    else      console.log("📡 Subscribed to esp32/vital");
  });

  // Topic riêng cho fall detection (tần số cao hơn)
  client.subscribe("esp32/fall", (err) => {
    if (err) console.log("❌ Subscribe fall error:", err.message);
    else      console.log("📡 Subscribed to esp32/fall");
  });
});

client.on("message", async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    console.log(`📥 MQTT [${topic}]:`, data);

    // ── Xử lý HR + SpO2 ──
    if (topic === "esp32/vital") {
      if (data.hr == null || data.spo2 == null) {
        console.log("❌ Invalid vital data (missing hr or spo2)");
        return;
      }

      const newVital = await Vital.create({ hr: data.hr, spo2: data.spo2 });
      console.log("✅ Vital saved:", newVital);

      io.emit("vital-data", newVital);
      console.log("📡 Vital emitted to clients");
    }

    // ── Xử lý Fall Detection ──
    else if (topic === "esp32/fall") {
      if (data.fall_detected == null) {
        console.log("❌ Invalid fall data (missing fall_detected)");
        return;
      }

      const newFall = await Fall.create({ fall_detected: Boolean(data.fall_detected) });
      console.log("✅ Fall saved:", newFall);

      // Chỉ emit lên socket khi ngã thật sự (true) để tránh flood client
      if (newFall.fall_detected) {
        io.emit("fall-alert", newFall);
        console.log("🚨 Fall alert emitted to clients");
      } else {
        io.emit("fall-data", newFall);
      }
    }

  } catch (err) {
    console.log("❌ MQTT message error:", err.message);
  }
});

// ===== API =====

// --- Vital (HR + SpO2) ---

// POST test
app.post("/api/vital", async (req, res) => {
  try {
    const { hr, spo2 } = req.body;

    if (hr == null || spo2 == null) {
      return res.status(400).json({ error: "Missing hr or spo2" });
    }

    const newVital = await Vital.create({ hr, spo2 });
    io.emit("vital-data", newVital);

    res.json({ message: "OK", data: newVital });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET lịch sử vital
app.get("/api/vital", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const data = await Vital.find().sort({ time: -1 }).limit(limit);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Fall Detection ---

// POST test
app.post("/api/fall", async (req, res) => {
  try {
    const { fall_detected } = req.body;

    if (fall_detected == null) {
      return res.status(400).json({ error: "Missing fall_detected" });
    }

    const newFall = await Fall.create({ fall_detected: Boolean(fall_detected) });

    if (newFall.fall_detected) {
      io.emit("fall-alert", newFall);
    } else {
      io.emit("fall-data", newFall);
    }

    res.json({ message: "OK", data: newFall });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET lịch sử fall (chỉ lấy các lần ngã thật)
app.get("/api/fall", async (req, res) => {
  try {
    const onlyFalls = req.query.only_falls === "true";
    const limit     = parseInt(req.query.limit) || 50;

    const filter = onlyFalls ? { fall_detected: true } : {};
    const data   = await Fall.find(filter).sort({ time: -1 }).limit(limit);

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===== SOCKET =====
io.on("connection", (socket) => {
  console.log("📡 Client connected:", socket.id);

  socket.on("disconnect", () => {
    console.log("📴 Client disconnected:", socket.id);
  });
});

// ===== RUN SERVER =====
server.listen(3000, () => {
  console.log("🚀 Server running on port 3000");
});