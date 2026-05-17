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
mongoose.connect("mongodb+srv://Hoaidat:hoaidat2004@cluster0.iarmzxb.mongodb.net/iot")
  .then(() => console.log("✅ MongoDB connected"))
  .catch(err => console.log("❌ DB error:", err));

// ===== MODELS =====
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
// Tạo model từ schema
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

//====== Sent email when fall detected =====
const sendFallEmail = async () => {
    const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
            user: "emb.datlh@gmail.com",
            pass: "kkfb nunj wkrp ctad"
        }
    });
    await transporter.sendMail({
        from: "emb.datlh@gmail.com",
        to: 'lehoaidat1603@gmail.com',
        subject: '⚠️ Cảnh báo: Phát hiện ngã!',
        text: 'Hệ thống phát hiện bệnh nhân A bị ngã. Vui lòng kiểm tra ngay!',
    });

    console.log('✅ Email cảnh báo đã gửi!_Fall');
};
// ====== sent email when HR abnormal =====
const sendHRAbnormalEmail = async () => {
    const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
            user: "emb.datlh@gmail.com",
            pass: "kkfb nunj wkrp ctad"
        }
    });
    await transporter.sendMail({
        from: "emb.datlh@gmail.com",
        to: 'lehoaidat1603@gmail.com',
        subject: '⚠️ Cảnh báo: Phát hiện nhịp tim bất thường!',
        text: 'Hệ thống phát hiện bệnh nhân A có nhịp tim bất thường. Vui lòng kiểm tra ngay!',
    });

    console.log('✅ Email cảnh báo đã gửi!_HR');
}

// ================= sent email when spo2 abnormal =====
const sendSpO2AbnormalEmail = async () => {
    const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
            user: "emb.datlh@gmail.com",
            pass: "kkfb nunj wkrp ctad"
        }
    });
    await transporter.sendMail({
        from: "emb.datlh@gmail.com",
        to: 'lehoaidat1603@gmail.com',
        subject: '⚠️ Cảnh báo: Phát hiện SpO2 bất thường!',
        text: 'Hệ thống phát hiện bệnh nhân A có SpO2 bất thường. Vui lòng kiểm tra ngay!',
    });

    console.log('✅ Email cảnh báo đã gửi!_SpO2');
}



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
      lastReceivedTime = Date.now(); // ← cập nhật thời gian nhận gần nhất
      //ham luu vao windows
      if(data.hr != 0){ 
        hrWindow.push(data.hr);
        if (hrWindow.length > WINDOW_SIZE) hrWindow.shift();
      }
      if(data.spo2 != 0) {
        spo2Window.push(data.spo2);
        if (spo2Window.length > WINDOW_SIZE) spo2Window.shift();
      }
      //kiem tra bat thuong
      if (checkwindow()) {
        const now = Date.now();
        if (now - lastSentTime_hr >= COOLDOWN_MS) {
          lastSentTime_hr = now;
          await sendHRAbnormalEmail();
        } else {
          console.log("⏳ HR abnormal but in cooldown period, email not sent");
        }
      }
      if (checkSpO2Window()) {
        const now = Date.now(); 
        if (now - lastSentTime_spo2 >= COOLDOWN_MS) {
          lastSentTime_spo2 = now;
          await sendSpO2AbnormalEmail();
        } else {
          console.log("⏳ SpO2 abnormal but in cooldown period, email not sent");
        }   
      }
      if(data.hr != 0 || data.spo2 != 0) {
      const newVital = await Vital.create({ hr: data.hr, spo2: data.spo2 });
      console.log("✅ Vital saved:", newVital);
      }
    }

    // ── Xử lý Fall Detection ──
    else if (topic === "esp32/fall") {
      if (data.fall_detected == null) {
        console.log("❌ Invalid fall data (missing fall_detected)");
        return;
      }
      const newFall = await Fall.create({ fall_detected: Boolean(data.fall_detected) });
      console.log("✅ Fall saved:", newFall);

      // Nếu phát hiện ngã, gửi email cảnh báo
      if (data.fall_detected) {
        await sendFallEmail();
      }
    }

  } catch (err) {
    console.log("❌ MQTT message error:", err.message);
  }
});

// ===== API =====
app.get("/api/vital", async (req, res) => {
    const limit = parseInt(req.query.limit) || 0;
    const data = await Vital.find().sort({ time: -1 }).limit(limit);
    res.json(data);
});
app.get("/api/fall", async (req, res) => {
    const onlyFalls = req.query.only_falls === "true";
    const limit     = parseInt(req.query.limit) || 0;
    const filter = onlyFalls ? { fall_detected: true } : {};
    const data   = await Fall.find(filter).sort({ time: -1 }).limit(limit);
    res.json(data);
});


//kiem tra bat thuong cua HR trong sliding window
function checkwindow() {
  if (hrWindow.length < WINDOW_SIZE) return null;
  let abnormalCount = 0;
  for(let i = 0; i < WINDOW_SIZE; i++) {
    if (hrWindow[i] < 50 || hrWindow[i] > 120) {
      abnormalCount++;
    }
  }
  if (abnormalCount >= WINDOW_SIZE*0.6) return true;
  return false;
}
//kiem tra bat thuong cua SpO2 trong sliding window
function checkSpO2Window() {
  if (spo2Window.length < WINDOW_SIZE) return null; 
  let abnormalCount = 0;
  for(let i = 0; i < WINDOW_SIZE; i++) {
    if (spo2Window[i] < 95) { 
      abnormalCount++;
    }     
  }
  if (abnormalCount >= WINDOW_SIZE*0.6) return true; 
  return false;
} 

//////  Nếu quá 5 phút không nhận được data mới, reset sliding window để tránh cảnh báo sai do dữ liệu cũ
let lastReceivedTime = Date.now();
const RESET_TIMEOUT_MS = 30 * 1000; // 30 giây
// Chạy kiểm tra mỗi 30 giây
setInterval(() => {
  const now = Date.now();
  if (now - lastReceivedTime >= RESET_TIMEOUT_MS) {
    if (hrWindow.length > 0) { // chỉ log khi window đang có data
      console.log("⏰ Quá 30 giây không nhận data, reset window!");
      lastSentTime_hr = 0; // reset cooldown để có thể gửi cảnh báo ngay khi có data mới
      lastSentTime_spo2 = 0;
      hrWindow.length = 0;
      spo2Window.length = 0;
    }
  }
}, 15 * 1000); // kiểm tra mỗi 15 giây




// ===== RUN SERVER =====
app.listen(3000, () => {
  console.log("🚀 Server running on port 3000");
});
