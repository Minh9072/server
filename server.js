console.log("🔥 SERVER START");

require("dotenv").config();

const express = require("express");
const http = require("http");
const mongoose = require("mongoose");
const { Server } = require("socket.io");
const mqtt = require("mqtt");
const cors = require("cors");

// ===== INIT =====
const app = express();
app.use(express.json());
app.use(cors());

// ⚠️ Render dùng PORT động
const PORT = process.env.PORT || 3000;

const server = http.createServer(app);

// ⚠️ Fix socket cho production
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// ===== CONNECT MONGODB =====
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log("✅ MongoDB connected"))
  .catch(err => console.log("❌ DB error:", err));

// ===== SCHEMAS =====
const VitalSchema = new mongoose.Schema({
  hr: Number,
  spo2: Number,
  time: { type: Date, default: Date.now }
});

const FallSchema = new mongoose.Schema({
  fall_detected: { type: Boolean, required: true },
  time: { type: Date, default: Date.now }
});

const Vital = mongoose.model("Vital", VitalSchema);
const Fall  = mongoose.model("Fall",  FallSchema);

// ===== MQTT CONNECT =====
const client = mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS
});

client.on("connect", () => {
  console.log("✅ MQTT connected");

  client.subscribe("esp32/vital");
  client.subscribe("esp32/fall");
});

client.on("message", async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    console.log(`📥 MQTT [${topic}]:`, data);

    if (topic === "esp32/vital") {
      if (data.hr == null || data.spo2 == null) return;

      const newVital = await Vital.create({
        hr: data.hr,
        spo2: data.spo2
      });

      io.emit("vital-data", newVital);
    }

    else if (topic === "esp32/fall") {
      if (data.fall_detected == null) return;

      const newFall = await Fall.create({
        fall_detected: Boolean(data.fall_detected)
      });

      if (newFall.fall_detected) {
        io.emit("fall-alert", newFall);
      } else {
        io.emit("fall-data", newFall);
      }
    }

  } catch (err) {
    console.log("❌ MQTT message error:", err.message);
  }
});

// ===== API =====

// Health check (Render cần)
app.get("/", (req, res) => {
  res.send("Server is running 🚀");
});

// --- Vital ---
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

app.get("/api/vital", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const data = await Vital.find().sort({ time: -1 }).limit(limit);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Fall ---
app.post("/api/fall", async (req, res) => {
  try {
    const { fall_detected } = req.body;

    if (fall_detected == null) {
      return res.status(400).json({ error: "Missing fall_detected" });
    }

    const newFall = await Fall.create({
      fall_detected: Boolean(fall_detected)
    });

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

app.get("/api/fall", async (req, res) => {
  try {
    const onlyFalls = req.query.only_falls === "true";
    const limit = parseInt(req.query.limit) || 50;

    const filter = onlyFalls ? { fall_detected: true } : {};
    const data = await Fall.find(filter)
      .sort({ time: -1 })
      .limit(limit);

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
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});

// ===== KEEP SERVER NOT TO SLEEP ======
setInterval(() => {
  console.log("🔄 Keep alive ping");

  fetch(process.env.SELF_URL || "https://your-app.onrender.com/")
    .then(res => console.log("keep-alive OK"))
    .catch(err => console.log("keep-alive error"));
}, 10 * 60 * 1000); // 10 phút
