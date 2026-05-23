console.log("🔥 SERVER START");

require("dotenv").config();

// ===== IMPORTS =====
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const mqtt = require("mqtt");
const nodemailer = require("nodemailer");

// ===== SLIDING WINDOW (in-memory) =====
const WINDOW_SIZE = 10
const hrWindow = []
const spo2Window = []
let lastSentTime_hr = 0;
let lastSentTime_spo2 = 0;
const COOLDOWN_MS = 2 * 60 * 1000;

// ===== INIT =====
const app = express();

app.use(express.json());
app.use(cors());

// ===== ENV =====
const PORT = process.env.PORT || 3000;

const {
  MONGO_URI,

  MQTT_URL,
  MQTT_USER,
  MQTT_PASS,

  EMAIL_USER,
  EMAIL_PASS,
  ALERT_EMAIL_TO2,
  RESEND_API_KEY2,
} = process.env;


// ===== CONNECT MONGODB =====
mongoose
  .connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB connected"))
  .catch((err) => console.log("❌ MongoDB error:", err.message));

// ===== MODELS =====

// HR + SpO2
const VitalSchema = new mongoose.Schema({
  hr: Number,
  spo2: Number,
  time: {
    type: Date,
    default: Date.now,
  },
});

// Fall detection
const FallSchema = new mongoose.Schema({
  fall_detected: {
    type: Boolean,
    required: true,
  },
  time: {
    type: Date,
    default: Date.now,
  },
});

const Vital = mongoose.model("Vital", VitalSchema);
const Fall = mongoose.model("Fall", FallSchema);

// ===== Resend =====
const { Resend } = require("resend");

const resend = new Resend(process.env.RESEND_API_KEY2);

const sendMail = async (subject, text) => {
  try {
    await resend.emails.send({
      from: 'VitalWatch <onboarding@resend.dev>',
      to: ALERT_EMAIL_TO2,
      subject,
      text,
    });

    console.log("✅ Email sent:", subject);
  } catch (err) {
    console.log("❌ Email error:", err.message);
  }
};


app.post('/api/contact', async (req, res) => {
  try {
    const { name, email, subject, message } = req.body

    const data = await resend.emails.send({
      from: 'VitalWatch <onboarding@resend.dev>',
      to: ALERT_EMAIL_TO2,
      subject: `Contact: ${subject}`,
      reply_to: email,

      html: `
        <h2>Liên hệ mới từ website</h2>

        <p><strong>Họ tên:</strong> ${name}</p>
        <p><strong>Email:</strong> ${email}</p>
        <p><strong>Chủ đề:</strong> ${subject}</p>

        <hr />

        <p>${message}</p>
      `,
    })

    res.status(200).json(data)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: 'Send mail failed' })
  }
})

// // ===== EMAIL FUNCTIONS =====

// const sendMail = async (subject, text) => {
//   try {
//     await transporter.sendMail({
//       from: EMAIL_USER,
//       to: ALERT_EMAIL_TO,
//       subject,
//       text,
//     });

//     console.log("✅ Email sent:", subject);
//   } catch (err) {
//     console.log("❌ Email error:", err.message);
//   }
// };

const sendFallEmail = async () => {
  await sendMail(
    "⚠️ Cảnh báo: Phát hiện ngã!",
    "Hệ thống phát hiện bệnh nhân A bị ngã. Vui lòng kiểm tra ngay!"
  );
};

const sendHRAbnormalEmail = async () => {
  await sendMail(
    "⚠️ Cảnh báo: Nhịp tim bất thường!",
    "Hệ thống phát hiện bệnh nhân A có nhịp tim bất thường."
  );
};

const sendSpO2AbnormalEmail = async () => {
  await sendMail(
    "⚠️ Cảnh báo: SpO2 bất thường!",
    "Hệ thống phát hiện bệnh nhân A có SpO2 bất thường."
  );
};

// ===== MQTT CONNECT =====
const client = mqtt.connect(MQTT_URL, {
  username: MQTT_USER,
  password: MQTT_PASS,
  reconnectPeriod: 5000,
});

client.on("connect", () => {
  console.log("✅ MQTT connected");

  client.subscribe("esp32/vital", (err) => {
    if (err) {
      console.log("❌ Subscribe vital error:", err.message);
    } else {
      console.log("📡 Subscribed: esp32/vital");
    }
  });

  client.subscribe("esp32/fall", (err) => {
    if (err) {
      console.log("❌ Subscribe fall error:", err.message);
    } else {
      console.log("📡 Subscribed: esp32/fall");
    }
  });
});

client.on("error", (err) => {
  console.log("❌ MQTT error:", err.message);
});

// ===== DATA TIMEOUT =====
let lastReceivedTime = Date.now();

const RESET_TIMEOUT_MS = 30 * 1000;

// ===== MQTT MESSAGE =====
client.on("message", async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    console.log(`📥 MQTT [${topic}]`, data);

    // ===== VITAL =====
    if (topic === "esp32/vital") {
      if (data.hr == null || data.spo2 == null) {
        console.log("❌ Invalid vital data");
        return;
      }

      lastReceivedTime = Date.now();

      // ===== HR WINDOW =====
      if (data.hr !== 0) {
        hrWindow.push(data.hr);

        if (hrWindow.length > WINDOW_SIZE) {
          hrWindow.shift();
        }
      }

      // ===== SPO2 WINDOW =====
      if (data.spo2 !== 0) {
        spo2Window.push(data.spo2);

        if (spo2Window.length > WINDOW_SIZE) {
          spo2Window.shift();
        }
      }

      // ===== HR ALERT =====
      if (checkHRWindow()) {
        const now = Date.now();

        if (now - lastSentTime_hr >= COOLDOWN_MS) {
          lastSentTime_hr = now;

          await sendHRAbnormalEmail();
        }
      }

      // ===== SPO2 ALERT =====
      if (checkSpO2Window()) {
        const now = Date.now();

        if (now - lastSentTime_spo2 >= COOLDOWN_MS) {
          lastSentTime_spo2 = now;

          await sendSpO2AbnormalEmail();
        }
      }

      // ===== SAVE DB =====
      if (data.hr !== 0 || data.spo2 !== 0) {
        const newVital = await Vital.create({
          hr: data.hr,
          spo2: data.spo2,
        });

        console.log("✅ Vital saved:", newVital._id);
      }
    }

    // ===== FALL =====
    else if (topic === "esp32/fall") {
      if (data.fall_detected == null) {
        console.log("❌ Invalid fall data");
        return;
      }

      const newFall = await Fall.create({
        fall_detected: Boolean(data.fall_detected),
      });

      console.log("✅ Fall saved:", newFall._id);

      if (data.fall_detected) {
        await sendFallEmail();
      }
    }
  } catch (err) {
    console.log("❌ MQTT message error:", err.message);
  }
});

// ===== CHECK HR =====
function checkHRWindow() {
  if (hrWindow.length < WINDOW_SIZE) return false;

  let abnormalCount = 0;

  for (let i = 0; i < WINDOW_SIZE; i++) {
    if (hrWindow[i] < 50 || hrWindow[i] > 120) {
      abnormalCount++;
    }
  }

  return abnormalCount >= WINDOW_SIZE * 0.6;
}

// ===== CHECK SPO2 =====
function checkSpO2Window() {
  if (spo2Window.length < WINDOW_SIZE) return false;

  let abnormalCount = 0;

  for (let i = 0; i < WINDOW_SIZE; i++) {
    if (spo2Window[i] < 95) {
      abnormalCount++;
    }
  }

  return abnormalCount >= WINDOW_SIZE * 0.6;
}

// ===== RESET WINDOW =====
setInterval(() => {
  const now = Date.now();

  if (now - lastReceivedTime >= RESET_TIMEOUT_MS) {
    if (hrWindow.length > 0 || spo2Window.length > 0) {
      console.log("⏰ No data received -> reset windows");

      hrWindow.length = 0;
      spo2Window.length = 0;

      lastSentTime_hr = 0;
      lastSentTime_spo2 = 0;
    }
  }
}, 15000);

// ===== ROUTES =====

app.get("/", (req, res) => {
  res.send("Server is running 🚀");
});

app.get("/api/vital", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 0;

    const data = await Vital.find()
      .sort({ time: -1 })
      .limit(limit);

    res.json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.get("/api/fall", async (req, res) => {
  try {
    const onlyFalls = req.query.only_falls === "true";

    const limit = parseInt(req.query.limit) || 0;

    const filter = onlyFalls
      ? { fall_detected: true }
      : {};

    const data = await Fall.find(filter)
      .sort({ time: -1 })
      .limit(limit);

    res.json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

// ===== DELETE ROUTES =====

// Xóa toàn bộ lịch sử vital (HR + SpO2)
app.delete("/api/vital", async (req, res) => {
  try {
    const result = await Vital.deleteMany({});
    res.json({
      message: "✅ Đã xóa toàn bộ dữ liệu vital",
      deletedCount: result.deletedCount,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Xóa toàn bộ lịch sử fall detection
app.delete("/api/fall", async (req, res) => {
  try {
    const result = await Fall.deleteMany({});
    res.json({
      message: "✅ Đã xóa toàn bộ dữ liệu fall",
      deletedCount: result.deletedCount,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===== EXPORT CSV =====

app.get("/api/vital/export", async (req, res) => {
  try {
    const data = await Vital.find().sort({ time: -1 });

    const rows = [
      ["Thời gian", "Nhịp tim (bpm)", "SpO2 (%)"],
      ...data.map(v => [
        new Date(v.time).toLocaleString("vi-VN"),
        v.hr,
        v.spo2,
      ]),
    ];

    const csv = rows.map(r => r.join(",")).join("\n");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="vital_history_${Date.now()}.csv"`);
    res.send("\uFEFF" + csv); // BOM để Excel đọc đúng tiếng Việt
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/fall/export", async (req, res) => {
  try {
    const data = await Fall.find().sort({ time: -1 });

    const rows = [
      ["Thời gian", "Phát hiện té ngã"],
      ...data.map(f => [
        new Date(f.time).toLocaleString("vi-VN"),
        f.fall_detected ? "Có" : "Không",
      ]),
    ];

    const csv = rows.map(r => r.join(",")).join("\n");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="fall_history_${Date.now()}.csv"`);
    res.send("\uFEFF" + csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===== START SERVER =====
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
