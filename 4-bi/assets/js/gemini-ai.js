/**
 * ==============================================================================
 * GEMINI-AI.JS — PHÂN TÍCH DỮ LIỆU TỰ ĐỘNG VỚI GOOGLE GEMINI AI
 * ==============================================================================
 * 1. Khóa API Gemini do người dùng tự cấu hình trong trình duyệt
 * 2. Phân tích 100% dựa trên dữ liệu thật được truy vấn từ Database
 * 3. Chế độ Fallback: Tự động suy luận toán học từ mảng SQL nếu mạng bị ngắt
 * ==============================================================================
 */

const GEMINI_CONFIG = {
  storageKey: 'NYC_TAXI_GEMINI_API_KEY',
  model: 'gemini-3.5-flash',
  endpoint: 'https://generativelanguage.googleapis.com/v1beta/models'
};

// Chỉ dùng khóa do người dùng tự nhập; không nhúng khóa dùng chung trong mã nguồn.
function getGeminiApiKey() {
  try {
    const savedKey = localStorage.getItem(GEMINI_CONFIG.storageKey);
    return savedKey && savedKey.trim() ? savedKey.trim() : '';
  } catch (_) {
    return '';
  }
}

function setGeminiApiKey(key) {
  try {
    if (key && key.trim()) {
      localStorage.setItem(GEMINI_CONFIG.storageKey, key.trim());
    } else {
      localStorage.removeItem(GEMINI_CONFIG.storageKey);
    }
  } catch (error) {
    console.warn('[Gemini AI] Không thể lưu khóa trong localStorage:', error.message);
  }
  updateGeminiStatusBadge();
}

function updateGeminiStatusBadge() {
  const dot = document.getElementById('geminiStatusDot');
  const hasKey = Boolean(getGeminiApiKey());
  if (dot) {
    dot.className = `ai-status-dot ${hasKey ? 'active' : ''}`;
    dot.title = hasKey ? `Gemini AI đã kích hoạt (${GEMINI_CONFIG.model})` : 'Chế độ suy luận dữ liệu cục bộ';
  }
}

/* ==============================================================================
 * 1. BỘ SUY LUẬN PHÂN TÍCH TOÁN HỌC TRỰC TIẾP TỪ DỮ LIỆU SQL (FALLBACK DỰ PHÒNG)
 * ============================================================================== */

function deriveTab0Insights(data) {
  if (!data || !data.kpis) return '<p>Đang chờ kết nối cơ sở dữ liệu Hive SQL để phân tích...</p>';
  const { kpis, monthly, borough, top_zones } = data;

  let maxMonth = { label: '—', count: 0 };
  let minMonth = { label: '—', count: Infinity };

  if (monthly && monthly.counts && monthly.counts.length) {
    monthly.counts.forEach((cnt, idx) => {
      if (cnt > maxMonth.count) maxMonth = { label: monthly.labels[idx], count: cnt };
      if (cnt > 0 && cnt < minMonth.count) minMonth = { label: monthly.labels[idx], count: cnt };
    });
  }

  const dropPercent = maxMonth.count > 0 && minMonth.count < Infinity
    ? (((maxMonth.count - minMonth.count) / maxMonth.count) * 100).toFixed(1)
    : 0;

  const topBoro = (borough && borough.labels && borough.labels[0]) || 'Manhattan';
  const topBoroPct = (borough && borough.percentages && borough.percentages[0]) || 0;
  const topZone = (top_zones && top_zones.labels && top_zones.labels[0]) || 'Upper East Side';
  const topZoneClaim = top_zones?.incremental_candidates
    ? 'Khu vực dẫn đầu trong các ứng viên đã lưu là'
    : 'Điểm nóng đón xe có lưu lượng lớn nhất là';

  return `
    <ul class="ai-insight-list">
      <li>
        <span class="ai-bullet">📌</span>
        <div>
          <strong>Quy mô & Phạm vi dữ liệu:</strong> Tổng sản lượng ghi nhận <span class="ai-num">${new Intl.NumberFormat('vi-VN').format(kpis.total_records)} chuyến</span> với tổng doanh thu cước <span class="ai-num">$${new Intl.NumberFormat('vi-VN').format(Math.round(kpis.total_revenue))}</span> trên toàn dải chuỗi thời gian <span class="ai-num">${kpis.period}</span> (${kpis.months_loaded} tháng).
        </div>
      </li>
      <li>
        <span class="ai-bullet">📈</span>
        <div>
          <strong>Biến động chu kỳ sản lượng:</strong> Sản lượng đạt đỉnh cao nhất vào tháng <span class="ai-num">${maxMonth.label}</span> với <span class="ai-num">${new Intl.NumberFormat('vi-VN').format(maxMonth.count)} chuyến</span>, và ghi nhận mức thấp nhất tại tháng <span class="ai-num">${minMonth.label}</span> (<span class="ai-num">${new Intl.NumberFormat('vi-VN').format(minMonth.count)} chuyến</span>, giảm <span class="ai-highlight-red">-${dropPercent}%</span> so với đỉnh do ảnh hưởng ngoại cảnh).
        </div>
      </li>
      <li>
        <span class="ai-bullet">📍</span>
        <div>
          <strong>Mật độ tập trung không gian:</strong> Quận <span class="ai-num">${topBoro}</span> chiếm ưu thế chi phối với <span class="ai-num">${topBoroPct}%</span> tổng thị phần taxi toàn thành phố. ${topZoneClaim} <span class="ai-num">${topZone}</span>.
        </div>
      </li>
    </ul>
  `;
}

function deriveTab1Insights(data) {
  if (!data || !data.hourly || !data.cancelled) return '<p>Đang chờ kết nối cơ sở dữ liệu Hive SQL...</p>';
  const { hourly, cancelled } = data;

  let maxFareHour = { h: '0h', val: 0 };
  let minFareHour = { h: '0h', val: Infinity };
  let maxDurHour = { h: '0h', val: 0 };

  if (hourly.avg_fare && hourly.avg_fare.length) {
    hourly.avg_fare.forEach((f, idx) => {
      if (f > maxFareHour.val) maxFareHour = { h: hourly.hours[idx], val: f };
      if (f > 0 && f < minFareHour.val) minFareHour = { h: hourly.hours[idx], val: f };
    });
  }

  if (hourly.avg_duration && hourly.avg_duration.length) {
    hourly.avg_duration.forEach((d, idx) => {
      if (d > maxDurHour.val) maxDurHour = { h: hourly.hours[idx], val: d };
    });
  }

  const fareDiffPct = minFareHour.val < Infinity
    ? (((maxFareHour.val - minFareHour.val) / minFareHour.val) * 100).toFixed(1)
    : 0;

  return `
    <ul class="ai-insight-list">
      <li>
        <span class="ai-bullet">💳</span>
        <div>
          <strong>Quy luật phân bổ giá cước 24h:</strong> Giá cước bình quân đạt đỉnh <span class="ai-num">$${maxFareHour.val.toFixed(2)} lúc ${maxFareHour.h}</span> (chênh lệch <span class="ai-num">+${fareDiffPct}%</span> so với mức thấp nhất <span class="ai-num">$${minFareHour.val.toFixed(2)} lúc ${minFareHour.h}</span>), phản ánh phụ phí đêm và cuốc xe đường dài đi sân bay.
        </div>
      </li>
      <li>
        <span class="ai-bullet">⏱️</span>
        <div>
          <strong>Thời lượng di chuyển & Ùn tắc:</strong> Thời gian hành trình dài nhất ghi nhận tại khung <span class="ai-num">${maxDurHour.h}</span> với <span class="ai-num">${maxDurHour.val.toFixed(1)} phút/cuốc</span>, phản ánh áp lực giao thông vào giờ cao điểm đô thị.
        </div>
      </li>
      <li>
        <span class="ai-bullet">⚠️</span>
        <div>
          <strong>Kiểm toán chất lượng dữ liệu:</strong> Phát hiện <span class="ai-num">${new Intl.NumberFormat('vi-VN').format(cancelled.total_cancelled)} chuyến</span> có giá cước âm hoặc bị hoàn tiền trên tổng <span class="ai-num">${new Intl.NumberFormat('vi-VN').format(cancelled.total_raw)} chuyến thô</span> (chiếm tỷ lệ <span class="ai-num">${cancelled.cancellation_rate.toFixed(2)}%</span>), được loại bỏ qua pipeline ETL để bảo vệ độ chính xác phân tích.
        </div>
      </li>
    </ul>
  `;
}

function deriveTab2Insights(data) {
  if (!data || !data.ml || !data.ml.metrics) return '<p>Đang chờ kết quả huấn luyện từ Spark MLlib...</p>';
  const { ml } = data;
  const { baseline, gbt } = ml.metrics;

  const rmseImp = ((baseline.rmse - gbt.rmse) / baseline.rmse) * 100;
  const maeImp = ((baseline.mae - gbt.mae) / baseline.mae) * 100;
  const mapeImp = ((baseline.mape - gbt.mape) / baseline.mape) * 100;
  const formatSignedPercent = value => {
    const rounded = Number(value.toFixed(1));
    if (rounded === 0) return '0.0%';
    return `${rounded > 0 ? '+' : '−'}${Math.abs(rounded).toFixed(1)}%`;
  };
  const direction = value => value > 0 ? 'giảm' : value < 0 ? 'tăng' : 'không đổi';
  const tone = value => value >= 0 ? 'ai-highlight-green' : 'ai-highlight-red';
  const allBetter = rmseImp > 0 && maeImp > 0 && mapeImp > 0;
  const allWorse = rmseImp < 0 && maeImp < 0 && mapeImp < 0;
  const scoreRelation = allBetter
    ? 'GBTRegressor tốt hơn Baseline trên cả ba chỉ số'
    : allWorse
      ? 'GBTRegressor có sai số cao hơn Baseline trên cả ba chỉ số'
      : 'Kết quả của GBTRegressor còn hỗn hợp giữa các chỉ số';
  const recommendation = allBetter
    ? 'Có thể cân nhắc triển khai sau khi kiểm thử thêm trên các giai đoạn dữ liệu khác.'
    : 'Chưa nên coi GBTRegressor là tối ưu; cần rà soát mô hình và kiểm thử thêm trước khi triển khai.';

  return `
    <ul class="ai-insight-list">
      <li>
        <span class="ai-bullet">🎯</span>
        <div>
          <strong>So sánh RMSE và MAE:</strong> RMSE ${direction(rmseImp)} từ <span class="ai-num">${baseline.rmse.toFixed(2)}</span> ${gbt.rmse < baseline.rmse ? 'xuống' : gbt.rmse > baseline.rmse ? 'lên' : 'ở mức'} <span class="ai-num">${gbt.rmse.toFixed(2)}</span> (<span class="${tone(rmseImp)}">${formatSignedPercent(rmseImp)}</span> theo công thức giảm lỗi); MAE ${direction(maeImp)} từ <span class="ai-num">${baseline.mae.toFixed(2)}</span> ${gbt.mae < baseline.mae ? 'xuống' : gbt.mae > baseline.mae ? 'lên' : 'ở mức'} <span class="ai-num">${gbt.mae.toFixed(2)}</span> (<span class="${tone(maeImp)}">${formatSignedPercent(maeImp)}</span>).
        </div>
      </li>
      <li>
        <span class="ai-bullet">📉</span>
        <div>
          <strong>Đánh giá MAPE:</strong> MAPE ${direction(mapeImp)} từ <span class="ai-num">${baseline.mape.toFixed(1)}%</span> ${gbt.mape < baseline.mape ? 'xuống' : gbt.mape > baseline.mape ? 'lên' : 'ở mức'} <span class="ai-num">${gbt.mape.toFixed(1)}%</span> (<span class="${tone(mapeImp)}">${formatSignedPercent(mapeImp)}</span> theo công thức giảm lỗi), ${gbt.mape < 20 ? 'dưới' : 'trên'} ngưỡng tham chiếu 20%.
        </div>
      </li>
      <li>
        <span class="ai-bullet">🧠</span>
        <div>
          <strong>Đánh giá triển khai:</strong> ${scoreRelation}. ${recommendation}
        </div>
      </li>
    </ul>
  `;
}

/* ==============================================================================
 * 2. TÍCH HỢP GOOGLE GEMINI 3.5 FLASH API
 * ============================================================================== */

async function callGeminiApi(promptText, apiKey) {
  const modelsToTry = [
    'gemini-3.5-flash',
    'gemini-3.5',
    'gemini-3.5-pro',
    'gemini-2.0-flash',
    'gemini-1.5-flash'
  ];
  const uniqueModels = [...new Set(modelsToTry)];
  let lastError = null;

  for (const model of uniqueModels) {
    try {
      const url = `${GEMINI_CONFIG.endpoint}/${model}:generateContent?key=${apiKey}`;
      const payload = {
        contents: [{
          parts: [{
            text: promptText
          }]
        }],
        generationConfig: {
          temperature: 0.2,
          maxOutputTokens: 600
        }
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error?.message || `HTTP ${response.status}`);
      }

      const result = await response.json();
      const rawText = result.candidates?.[0]?.content?.parts?.[0]?.text || '';
      if (rawText && rawText.trim()) {
        return { text: rawText, modelUsed: model };
      }
    } catch (err) {
      lastError = err;
      console.warn(`[Gemini AI] Thử model ${model} không thành công:`, err.message);
    }
  }

  throw lastError || new Error('Không thể kết nối tới Google Gemini API.');
}

function formatMarkdownToHtml(markdown) {
  const raw = String(markdown || '').replace(/```(?:markdown)?/gi, '').trim();
  const leakedPromptPattern = /\b(?:constraint\s*\d*|drafting(?:\s+section)?|trick\s*catch|the prompt states|chain[- ]of[- ]thought|internal reasoning|scratchpad)\b|(?:chỉ dẫn nội bộ|bản nháp|suy luận nội bộ)/i;
  if (!raw || leakedPromptPattern.test(raw)) return null;

  const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.length !== 3) return null;

  const escapeHtml = value => String(value).replace(/[&<>"']/g, character => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  })[character]);
  const items = [];

  for (const line of lines) {
    const match = line.match(/^(?:[-*•]\s+|\d+[.)]\s+)(.+)$/);
    if (!match) return null;

    const content = match[1].trim();
    const plainText = content.replace(/\*\*/g, '').replace(/[`*_]/g, '').trim();
    if (plainText.length < 25 || plainText.length > 500) return null;

    const safeContent = escapeHtml(content).replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    items.push(`<li><span class="ai-bullet">✨</span><div>${safeContent}</div></li>`);
  }

  return `<ul class="ai-insight-list">${items.join('')}</ul>`;
}

/* ==============================================================================
 * 3. HÀM KÍCH HOẠT PHÂN TÍCH CHO TỪNG TAB
 * ============================================================================== */

window.refreshAiInsights = async function(tabIndex, forceGemini = false) {
  const data = window.NYC_DATA;
  if (!data) return;

  const containerId = `aiInsightTab${tabIndex}`;
  const container = document.getElementById(containerId);
  const badgeId = `aiBadgeTab${tabIndex}`;
  const badgeEl = document.getElementById(badgeId);
  if (!container) return;

  const apiKey = getGeminiApiKey();

  // NẾU CÓ API KEY -> GỌI GEMINI 3.5 FLASH
  if (apiKey) {
    container.innerHTML = `
      <div class="ai-loading-box">
        <span class="ai-gemini-spinner"></span>
        <span>Google Gemini 3.5 đang phân tích dữ liệu thực tế từ Database...</span>
      </div>
    `;

    try {
      let prompt = '';
      if (tabIndex === 0) {
        prompt = `OUTPUT ONLY 3 bullet points in Vietnamese. Start each line with "* ". No preamble, no title, no explanation, no greetings. Only 3 lines total.

Data (NYC Yellow Taxi – Hive SQL):
- Tổng chuyến sạch: ${data.kpis.total_records}
- Doanh thu: $${data.kpis.total_revenue}
- Giá cước TB: $${data.kpis.avg_fare} | Thời gian TB: ${data.kpis.avg_duration} phút
- Giai đoạn: ${data.kpis.period} (${data.kpis.months_loaded} tháng)
- Thị phần quận: ${data.borough?.labels?.slice(0,3).map((l,i)=>`${l} ${data.borough.percentages[i]}%`).join(', ')}
- Top điểm đón${data.top_zones?.incremental_candidates ? ' (ứng viên từ cache tăng dần, không phải xếp hạng đầy đủ)' : ''}: ${data.top_zones?.labels?.slice(0,3).join(', ')}

Phân tích 3 điểm (mỗi điểm in đậm số liệu): (1) Quy mô & doanh thu, (2) Chu kỳ sản lượng, (3) Phân bổ không gian thành phố. Nếu Top điểm đón ghi là ứng viên từ cache, không khẳng định đó là xếp hạng chính xác toàn bộ lịch sử.`;

      } else if (tabIndex === 1) {
        const fares = data.hourly?.avg_fare || [];
        const durs  = data.hourly?.avg_duration || [];
        const hrs   = data.hourly?.hours || [];
        const maxFare = fares.length ? Math.max(...fares).toFixed(2) : '—';
        const maxFareH = fares.length ? (hrs[fares.indexOf(Math.max(...fares))] || '—') : '—';
        const maxDur  = durs.length ? Math.max(...durs).toFixed(1) : '—';
        const maxDurH = durs.length ? (hrs[durs.indexOf(Math.max(...durs))] || '—') : '—';
        prompt = `OUTPUT ONLY 3 bullet points in Vietnamese. Start each line with "* ". No preamble, no title, no explanation. Only 3 lines total.

Data (NYC Taxi Fare & Quality – Hive SQL):
- Giá cước cao nhất: $${maxFare} lúc ${maxFareH}
- Thời gian di chuyển lâu nhất: ${maxDur} phút lúc ${maxDurH}
- Bản ghi thô: ${data.cancelled.total_raw} | Chuyến hủy/âm cước: ${data.cancelled.total_cancelled} (${data.cancelled.cancellation_rate}%)

Phân tích 3 điểm (in đậm số liệu): (1) Quy luật giá cước 24h & nguyên nhân, (2) Ùn tắc giờ cao điểm, (3) Kiểm toán chất lượng & rủi ro hoàn cước.`;

      } else if (tabIndex === 2) {
        const b = data.ml.metrics.baseline, g = data.ml.metrics.gbt;
        const formatDelta = (baseline, model) => {
          if (!baseline) return 'không xác định';
          const delta = ((baseline - model) / baseline) * 100;
          const rounded = Number(delta.toFixed(1));
          if (rounded === 0) return '0.0% (không đổi)';
          return `${rounded > 0 ? '+' : '−'}${Math.abs(rounded).toFixed(1)}% (${rounded > 0 ? 'giảm lỗi' : 'tăng lỗi'})`;
        };
        prompt = `OUTPUT ONLY 3 bullet points in Vietnamese. Start each line with "* ". No preamble, no title, no explanation. Only 3 lines total.

Data (Spark ML – GBTRegressor vs Baseline):
- Baseline: RMSE=${b.rmse}, MAE=${b.mae}, MAPE=${b.mape}%
- GBTRegressor: RMSE=${g.rmse}, MAE=${g.mae}, MAPE=${g.mape}%
- Mức giảm/tăng sai số (số dương = giảm lỗi, số âm = tăng lỗi): RMSE ${formatDelta(b.rmse, g.rmse)}, MAE ${formatDelta(b.mae, g.mae)}, MAPE ${formatDelta(b.mape, g.mape)}
- Điểm đánh giá: ${data.ml.chart_data?.location} | Train: ${data.ml.train_count} | Test: ${data.ml.test_count}

Phân tích 3 điểm (in đậm số liệu): (1) So sánh RMSE/MAE/MAPE, lưu ý chỉ số thấp hơn là tốt hơn và không mặc định GBT tốt hơn Baseline; (2) Đánh giá kết quả dựa đúng số liệu, không tự suy diễn nguyên nhân; (3) Khả năng triển khai thực tế.`;
      }

      const aiResult = await callGeminiApi(prompt, apiKey);
      const formattedResult = formatMarkdownToHtml(aiResult.text);
      if (!formattedResult) {
        throw new Error('Gemini trả về nội dung không đúng định dạng phân tích.');
      }
      container.innerHTML = formattedResult;
      container.dataset.hasGemini = 'true';
      if (badgeEl) {
        const modelName = aiResult.modelUsed.includes('3.5') ? 'Google Gemini 3.5 Flash' : `Google Gemini (${aiResult.modelUsed})`;
        badgeEl.textContent = `${modelName} • Live AI`;
        badgeEl.className = 'ai-badge-chip gemini-active';
      }
      return;
    } catch (err) {
      console.warn('[Gemini AI] Lỗi gọi API:', err.message);
    }
  }

  // FALLBACK: SUY LUẬN TỰ ĐỘNG TỪ MẢNG SỐ LIỆU SQL
  let outputHtml = '';
  if (tabIndex === 0) outputHtml = deriveTab0Insights(data);
  else if (tabIndex === 1) outputHtml = deriveTab1Insights(data);
  else if (tabIndex === 2) outputHtml = deriveTab2Insights(data);

  container.innerHTML = outputHtml;
  container.dataset.hasGemini = 'false';
  if (badgeEl) {
    badgeEl.textContent = 'Bộ suy luận SQL';
    badgeEl.className = 'ai-badge-chip';
  }
};

window.updateAllAiInsights = function() {
  window.refreshAiInsights(0);
  window.refreshAiInsights(1);
  window.refreshAiInsights(2);
};

/* ==============================================================================
 * 4. CẤU HÌNH HỘP THOẠI MODAL GEMINI API
 * ============================================================================== */

window.openGeminiModal = function() {
  const modal = document.getElementById('geminiModal');
  const input = document.getElementById('geminiApiKeyInput');
  if (input) input.value = getGeminiApiKey();
  if (modal) modal.classList.add('open');
};

window.closeGeminiModal = function() {
  const modal = document.getElementById('geminiModal');
  if (modal) modal.classList.remove('open');
};

window.saveGeminiApiKeyFromModal = function() {
  const input = document.getElementById('geminiApiKeyInput');
  const key = input ? input.value : '';
  setGeminiApiKey(key);
  closeGeminiModal();
  updateGeminiStatusBadge();
  window.updateAllAiInsights();
};

document.addEventListener('DOMContentLoaded', () => {
  updateGeminiStatusBadge();
});
