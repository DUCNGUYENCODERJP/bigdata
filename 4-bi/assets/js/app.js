/**
 * ==============================================================================
 * APP.JS — ĐIỀU KHIỂN GIAO DIỆN & TRUY VẤN DỮ LIỆU TRỰC TIẾP TỪ HIVE SQL
 * ==============================================================================
 * Toàn bộ dữ liệu được truy vấn 100% động từ Database (Data Warehouse Hive SQL).
 * Không sử dụng dữ liệu tĩnh hay số liệu mẫu gán sẵn trong mã nguồn.
 * ==============================================================================
 */

let dashboardDataVersion = null;
let dashboardLoadInFlight = false;
let cacheStatusPollInFlight = false;
let lastShownIncrementalError = null;

/* ==============================================================================
 * HÀM CHUYỂN ĐỔI TAB ĐIỀU HƯỚNG
 * ============================================================================== */
function switchTab(index) {
  document.querySelectorAll('.tab-btn').forEach((btn, i) => {
    btn.classList.toggle('active', i === index);
    btn.setAttribute('aria-selected', i === index ? 'true' : 'false');
  });

  document.querySelectorAll('.tab-pane').forEach((pane, i) => {
    pane.classList.toggle('active', i === index);
  });

  // Tự động căn chỉnh kích thước biểu đồ Chart.js khi hiển thị tab
  window.dispatchEvent(new Event('resize'));

  // Cập nhật khối phân tích AI cho tab hiện tại nếu có dữ liệu
  if (window.refreshAiInsights && window.NYC_DATA) {
    window.refreshAiInsights(index);
  }
}

/* ==============================================================================
 * TIỆN ÍCH ĐỊNH DẠNG SỐ LIỆU TỪ DATABASE
 * ============================================================================== */
function setEl(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function setStorageSize(text, source) {
  const el = document.getElementById('contextStorageSize');
  if (!el) return;
  const sourceRank = { fallback: 0, snapshot: 1, api: 2 };
  if ((sourceRank[el.dataset.source] ?? -1) > (sourceRank[source] ?? -1)) return;
  el.textContent = text;
  el.dataset.source = source;
}

function loadStorageSizeFromSnapshot(kpis) {
  if (kpis?.partition !== 'all') {
    setStorageSize('Chưa có metadata HDFS', 'fallback');
    return;
  }

  setStorageSize('Đang lấy từ snapshot...', 'fallback');
  Promise.all([
    fetch('/api/status', { cache: 'no-store' }).then(response => response.ok ? response.json() : Promise.reject(new Error('Trạng thái API không truy cập được'))),
    fetch('/cache/dashboard_hive_cache.json', { cache: 'no-store' }).then(response => response.ok ? response.json() : Promise.reject(new Error('Snapshot không truy cập được')))
  ])
    .then(([status, snapshot]) => {
      const identityMatches = snapshot?.version === 1
        && snapshot?.identity?.partition === 'all'
        && snapshot.identity.warehouse_host === status?.warehouse_host
        && snapshot.identity.partition === status?.partition
        && snapshot.identity.clean_table === status?.clean_table
        && snapshot.identity.raw_table === status?.raw_table
        && snapshot.data?.kpis?.partition === status?.partition;
      const cachedKpis = identityMatches ? snapshot?.data?.kpis : null;
      if (!cachedKpis) {
        setStorageSize('Chưa có metadata HDFS', 'fallback');
        return;
      }

      const size = cachedKpis.storage_size || (
        Number.isFinite(cachedKpis.storage_bytes) && cachedKpis.storage_bytes > 0
          ? `${(Number(cachedKpis.storage_bytes) / (1024 ** 3)).toFixed(2)} GiB Parquet (Snappy)`
          : null
      );
      setStorageSize(size ? `${size} • snapshot` : 'Chưa có metadata HDFS', 'snapshot');
    })
    .catch(() => setStorageSize('Chưa có metadata HDFS', 'fallback'));
}

function formatCurrencyBillions(num) {
  if (!Number.isFinite(num)) return '—';
  if (num >= 1000000000) {
    return `$${(num / 1000000000).toFixed(2)} Tỷ`;
  }
  return `$${new Intl.NumberFormat('vi-VN').format(Math.round(num))}`;
}

function formatTrips(num) {
  if (!Number.isFinite(num)) return '—';
  if (num >= 1000000000) {
    return `${(num / 1000000000).toFixed(3)} Tỷ`;
  }
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(2)} Triệu`;
  }
  return new Intl.NumberFormat('vi-VN').format(num);
}

function setImprovementTag(id, improvement) {
  const tag = document.getElementById(id);
  if (!tag) return;
  if (!Number.isFinite(improvement)) {
    tag.textContent = 'Không xác định';
    tag.className = 'kpi-tag tag-blue';
  } else if (improvement > 0) {
    tag.textContent = 'Tốt hơn';
    tag.className = 'kpi-tag tag-green';
  } else if (improvement < 0) {
    tag.textContent = 'Sai số tăng';
    tag.className = 'kpi-tag tag-amber';
  } else {
    tag.textContent = 'Không đổi';
    tag.className = 'kpi-tag tag-blue';
  }
}

/* ==============================================================================
 * CẬP NHẬT CÁC CON SỐ KPI VÀ THÔNG SỐ VÀO GIAO DIỆN TỪ DỮ LIỆU SQL
 * ============================================================================== */
function updateKPIs(data) {
  if (!data || !data.kpis) return;
  const { kpis, cancelled, ml } = data;

  // Phạm vi dữ liệu
  setEl('contextPeriodText', `${kpis.period} (${kpis.months_loaded}/${kpis.months_expected} tháng)`);
  if (kpis.storage_size) {
    setStorageSize(kpis.storage_size, 'api');
  } else if (kpis.storage_bytes) {
    setStorageSize(`${(kpis.storage_bytes / (1024**3)).toFixed(2)} GiB Parquet (Snappy)`, 'api');
  } else {
    const storageEl = document.getElementById('contextStorageSize');
    if (storageEl) storageEl.dataset.source = 'fallback';
    loadStorageSizeFromSnapshot(kpis);
  }

  // Tab 0: Nhu cầu tổng quan
  setEl('kpiTotalRecords', formatTrips(kpis.total_records));
  setEl('kpiRevenue', formatCurrencyBillions(kpis.total_revenue));
  setEl('kpiAvgFare', `$${Number(kpis.avg_fare).toFixed(2)}`);
  setEl('kpiAvgDuration', `${Number(kpis.avg_duration).toFixed(1)} phút`);
  setEl('kpiPartition', `${kpis.period} • ${kpis.months_loaded}/${kpis.months_expected} tháng dữ liệu`);
  setEl('monthlySubtitle', `Phạm vi ${kpis.period} • ${kpis.months_loaded}/${kpis.months_expected} tháng dữ liệu từ Hive SQL`);

  // Tab 1: Bản ghi raw & chuyến hủy
  if (cancelled) {
    setEl('kpiRawRecords', formatTrips(cancelled.total_raw));
    setEl('kpiCancelledRecords', formatTrips(cancelled.total_cancelled));
    setEl('kpiCancellationRate', `${Number(cancelled.cancellation_rate).toFixed(2)}%`);
    const rawNegativeFareTotal = cancelled.negative_fare_total;
    const negativeFareTotal = Number(rawNegativeFareTotal);
    const hasExactNegativeFareTotal = rawNegativeFareTotal !== null
      && rawNegativeFareTotal !== undefined
      && Number.isFinite(negativeFareTotal);
    setEl(
      'kpiRefundAmount',
      hasExactNegativeFareTotal ? `-$${(negativeFareTotal / 1000000).toFixed(1)} Triệu` : '—'
    );
    setEl(
      'kpiNegativeFareNote',
      hasExactNegativeFareTotal
        ? 'Tổng giá trị cước âm hoặc hoàn tiền trong dữ liệu gốc'
        : 'Cache lịch sử chưa lưu tổng này; không quét lại dữ liệu cũ để tính.'
    );
  }

  const topZonesNote = document.getElementById('topZonesCacheNote');
  if (topZonesNote) {
    const approximate = Boolean(data.top_zones?.incremental_candidates);
    topZonesNote.hidden = !approximate;
    topZonesNote.textContent = approximate
      ? 'Ước tính từ Top 8 đã lưu và các tháng mới; cache cũ không có phân bố đầy đủ của mọi khu vực.'
      : '';
  }

  // Tab 2: Kết quả đánh giá mô hình Machine Learning (Spark ML)
  if (ml && ml.metrics) {
    const { baseline, gbt } = ml.metrics;
    if (baseline && gbt) {
      // Positive means lower error than baseline; negative means higher error.
      const improvement = (base, model) => (
        Number.isFinite(base) && Number.isFinite(model) && base > 0
          ? ((base - model) / base) * 100
          : null
      );
      const formatImprovement = value => {
        if (!Number.isFinite(value)) return '—';
        const rounded = Math.abs(value) < 0.05 ? 0 : value;
        const sign = rounded > 0 ? '+' : rounded < 0 ? '−' : '';
        return `${sign}${Math.abs(rounded).toFixed(1)}%`;
      };
      const rmseImp = improvement(baseline.rmse, gbt.rmse);
      const maeImp = improvement(baseline.mae, gbt.mae);
      const mapeImp = improvement(baseline.mape, gbt.mape);

      setEl('kpiRmseImprove', formatImprovement(rmseImp));
      setEl('kpiMaeImprove', formatImprovement(maeImp));
      setEl('kpiMapeImprove', formatImprovement(mapeImp));
      setImprovementTag('kpiRmseTag', rmseImp);
      setImprovementTag('kpiMaeTag', maeImp);
      setImprovementTag('kpiMapeTag', mapeImp);

      setEl('tblBaseRmse', Number(baseline.rmse).toFixed(2));
      setEl('tblBaseMae', Number(baseline.mae).toFixed(2));
      setEl('tblBaseMape', `${Number(baseline.mape).toFixed(2)}%`);

      setEl('tblGbtRmse', Number(gbt.rmse).toFixed(2));
      setEl('tblGbtMae', Number(gbt.mae).toFixed(2));
      setEl('tblGbtMape', `${Number(gbt.mape).toFixed(2)}%`);

      setEl('kpiBaselineRmseSub', `RMSE ${gbt.rmse < baseline.rmse ? 'giảm xuống' : gbt.rmse > baseline.rmse ? 'tăng lên' : 'giữ nguyên ở'} ${Number(gbt.rmse).toFixed(2)} (Baseline: ${Number(baseline.rmse).toFixed(2)})`);

      const metricComparisons = [
        { id: 'tblGbtRmse', baseline: baseline.rmse, model: gbt.rmse },
        { id: 'tblGbtMae', baseline: baseline.mae, model: gbt.mae },
        { id: 'tblGbtMape', baseline: baseline.mape, model: gbt.mape }
      ];
      const validComparisons = metricComparisons.filter(({ baseline: base, model }) =>
        Number.isFinite(base) && Number.isFinite(model)
      );
      const betterCount = validComparisons.filter(({ baseline: base, model }) => model < base).length;
      const worseCount = validComparisons.filter(({ baseline: base, model }) => model > base).length;
      metricComparisons.forEach(({ id, baseline: base, model }) => {
        const value = document.getElementById(id);
        if (!value) return;
        value.classList.toggle('stat-bold-green', model < base);
        value.classList.toggle('stat-bold-red', model > base);
        value.classList.toggle('stat-mono', model === base);
      });

      const rating = document.getElementById('gbtModelRating');
      const modelTag = document.getElementById('kpiModelTag');
      if (rating) {
        if (validComparisons.length !== metricComparisons.length) {
          rating.textContent = 'Thiếu chỉ số';
          rating.className = 'badge-baseline';
        } else if (betterCount === metricComparisons.length) {
          rating.textContent = '✓ Tốt hơn Baseline';
          rating.className = 'badge-optimal';
        } else if (betterCount > 0) {
          rating.textContent = 'Kết quả hỗn hợp';
          rating.className = 'badge-baseline';
        } else if (worseCount === metricComparisons.length) {
          rating.textContent = 'Sai số cao hơn';
          rating.className = 'badge-baseline';
        } else {
          rating.textContent = 'Kết quả hỗn hợp';
          rating.className = 'badge-baseline';
        }
      }
      if (modelTag) {
        if (validComparisons.length !== metricComparisons.length) {
          modelTag.textContent = 'Thiếu chỉ số';
          modelTag.className = 'kpi-tag tag-blue';
        } else if (betterCount === metricComparisons.length) {
          modelTag.textContent = 'Tối ưu';
          modelTag.className = 'kpi-tag tag-green';
        } else if (betterCount > 0) {
          modelTag.textContent = 'Kết quả hỗn hợp';
          modelTag.className = 'kpi-tag tag-blue';
        } else if (worseCount === metricComparisons.length) {
          modelTag.textContent = 'Cần rà soát';
          modelTag.className = 'kpi-tag tag-amber';
        } else {
          modelTag.textContent = 'Kết quả hỗn hợp';
          modelTag.className = 'kpi-tag tag-blue';
        }
      }
      const gbtRow = document.getElementById('gbtModelRow');
      if (gbtRow) gbtRow.classList.toggle(
        'highlight-row',
        validComparisons.length === metricComparisons.length && betterCount === metricComparisons.length
      );
    }

    if (ml.chart_data && ml.chart_data.location) {
      setEl('specModelZone', ml.chart_data.location);
      setEl('forecastSubtitle', `Khu vực: ${ml.chart_data.location} • Đánh giá trên 20% mẫu kiểm thử (Spark MLlib)`);
    }

    if (ml.train_count && ml.test_count) {
      setEl('specTrainTest', `${new Intl.NumberFormat('vi-VN').format(ml.train_count)} Train / ${new Intl.NumberFormat('vi-VN').format(ml.test_count)} Test`);
    }
  }
}

/* ==============================================================================
 * QUẢN LÝ TRẠNG THÁI KẾT NỐI VÀ CẢNH BÁO LỖI
 * ============================================================================== */
function hideStatus() {
  const statusEl = document.getElementById('dbConnectionStatus');
  if (!statusEl) return;
  statusEl.classList.add('fade-out');
  setTimeout(() => {
    statusEl.style.display = 'none';
    statusEl.classList.remove('fade-out');
  }, 350);
}

function updateStatus(message, state, allowRetry = false) {
  const statusEl = document.getElementById('dbConnectionStatus');
  if (!statusEl) return;

  statusEl.replaceChildren();
  statusEl.style.display = 'flex';
  statusEl.classList.remove('fade-out');

  // Icon trạng thái
  const iconSpan = document.createElement('span');
  iconSpan.className = 'status-icon';
  if (state === 'loading') {
    iconSpan.innerHTML = '<span class="spinner"></span>';
  } else {
    iconSpan.textContent = '⚠️';
  }
  statusEl.appendChild(iconSpan);

  // Nội dung thông báo
  const messageEl = document.createElement('span');
  messageEl.className = 'status-text';
  messageEl.textContent = message;
  statusEl.appendChild(messageEl);

  // Nút thử lại nếu có lỗi
  if (allowRetry) {
    const retryBtn = document.createElement('button');
    retryBtn.className = 'btn btn-ghost';
    retryBtn.style.padding = '2px 8px';
    retryBtn.style.fontSize = '12px';
    retryBtn.style.marginLeft = '8px';
    retryBtn.textContent = 'Thử lại';
    retryBtn.addEventListener('click', () => loadLiveDatabaseData());
    statusEl.appendChild(retryBtn);
  }

  // Nút đóng dấu nhân (x)
  const closeBtn = document.createElement('button');
  closeBtn.className = 'toast-close-btn';
  closeBtn.innerHTML = '&times;';
  closeBtn.title = 'Đóng thông báo';
  closeBtn.addEventListener('click', hideStatus);
  statusEl.appendChild(closeBtn);

  statusEl.className = `db-status-bar ${state}`;
}

function renderFullDashboard(data) {
  window.NYC_DATA = data;
  updateKPIs(data);
  if (window.renderHeatmap) window.renderHeatmap();
  if (window.initAllCharts) window.initAllCharts(data);
  if (window.updateAllAiInsights) window.updateAllAiInsights();
}

/* ==============================================================================
 * KẾT NỐI VÀ TẢI DỮ LIỆU TỪ HIVE SQL BACKEND (SERVER.PY TRUY VẤN DATABASE)
 * ============================================================================== */
async function loadLiveDatabaseData() {
  if (dashboardLoadInFlight) return;
  dashboardLoadInFlight = true;
  if (!window.NYC_DATA) {
    updateStatus('Đang nạp dữ liệu cache; server sẽ tự bổ sung dữ liệu mới từ Hive.', 'loading');
  }

  try {
    const response = await fetch('/api/data', { cache: 'no-store' });
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.details || data.error || `HTTP ${response.status}`);
    }
    if (!data.kpis || !data.cancelled || !data.ml) {
      throw new Error('API không trả về đủ dữ liệu truy vấn SQL.');
    }

    // Cập nhật giao diện với dữ liệu thực tế từ Database
    renderFullDashboard(data);
    const responseVersion = Number(data._cache_updated_at);
    if (Number.isFinite(responseVersion)) dashboardDataVersion = responseVersion;
    hideStatus();
  } catch (error) {
    console.error('[BI Dashboard] Lỗi kết nối Hive SQL:', error);
    if (!window.NYC_DATA) {
      updateStatus(`Không thể kết nối cơ sở dữ liệu Hive SQL: ${error.message}`, 'warning', true);
    } else {
      updateStatus(`Không làm mới được Hive SQL: ${error.message}. Dữ liệu đang hiển thị được giữ lại.`, 'warning', true);
    }
  } finally {
    dashboardLoadInFlight = false;
  }
}

async function pollCacheStatus() {
  if (cacheStatusPollInFlight) return;
  cacheStatusPollInFlight = true;
  try {
    const response = await fetch('/api/status', { cache: 'no-store' });
    if (!response.ok) return;
    const status = await response.json();
    if (status.storage_size) setStorageSize(status.storage_size, 'api');

    if (status.last_incremental_error && status.last_incremental_error !== lastShownIncrementalError) {
      lastShownIncrementalError = status.last_incremental_error;
      if (window.NYC_DATA) {
        updateStatus(`Chưa tự đồng bộ được partition mới: ${status.last_incremental_error}. Dữ liệu cache hiện tại vẫn dùng được.`, 'warning');
      }
    } else if (!status.last_incremental_error) {
      lastShownIncrementalError = null;
    }

    const latestVersion = Number(status.last_queried);
    if (
      window.NYC_DATA
      && Number.isFinite(latestVersion)
      && (dashboardDataVersion === null || latestVersion > dashboardDataVersion)
    ) {
      await loadLiveDatabaseData();
    }
  } catch (error) {
    console.warn('[BI Dashboard] Không đọc được trạng thái cache:', error);
  } finally {
    cacheStatusPollInFlight = false;
  }
}

/* ==============================================================================
 * KHỞI TẠO KHI TẢI XONG TRANG
 * ============================================================================== */
document.addEventListener('DOMContentLoaded', () => {
  // Nạp cache một lần; từ đó chỉ làm mới giao diện khi server đã gộp partition mới.
  loadLiveDatabaseData();
  window.setInterval(pollCacheStatus, 15000);
});
