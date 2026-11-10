/**
 * ==============================================================================
 * CHARTS.JS — HỆ THỐNG BIỂU ĐỒ TRỰC QUAN HÓA CHART.JS CHO NYC TAXI BI
 * ==============================================================================
 */

// Bộ lưu trữ các instance của biểu đồ để dọn dẹp khi cập nhật lại dữ liệu
let chartInstances = {};

// Cấu hình chung cho toàn bộ Chart.js
if (typeof Chart !== 'undefined') {
  Chart.defaults.font.family = "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
  Chart.defaults.color = '#94a3b8';
  Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.08)';
}

function formatChartPercent(value) {
  if (!Number.isFinite(value) || value < 0) return '0';
  if (value === 0) return '0';
  if (value >= 0.01) return value.toFixed(2);
  const decimalPlaces = Math.min(18, Math.max(3, Math.ceil(-Math.log10(value)) + 2));
  const formatted = value.toFixed(decimalPlaces).replace(/0+$/, '').replace(/\.$/, '');
  return Number(formatted) === 0 ? '<0.000000000000000001' : formatted;
}

function createTinySliceMarkerPlugin(values, colors) {
  return {
    id: 'tinySliceMarkers',
    afterDatasetsDraw(chart) {
      const total = values.reduce((sum, value) => sum + (Number(value) || 0), 0);
      if (!total) return;

      const arcs = chart.getDatasetMeta(0).data;
      const minimumAngle = 2 * Math.PI / 180;
      arcs.forEach((arc, index) => {
        if (!values[index] || (chart.getDataVisibility && !chart.getDataVisibility(index))) return;
        if (Math.abs(arc.endAngle - arc.startAngle) >= minimumAngle) return;

        const middleAngle = (arc.startAngle + arc.endAngle) / 2;
        const radius = (arc.innerRadius + arc.outerRadius) / 2;
        const ctx = chart.ctx;
        ctx.save();
        ctx.beginPath();
        ctx.arc(arc.x, arc.y, radius, middleAngle - minimumAngle / 2, middleAngle + minimumAngle / 2);
        ctx.strokeStyle = colors[index];
        ctx.lineWidth = arc.outerRadius - arc.innerRadius;
        ctx.lineCap = 'butt';
        ctx.stroke();
        ctx.restore();
      });
    }
  };
}

/* ==============================================================================
 * HÀM KHỞI TẠO TẤT CẢ BIỂU ĐỒ (INIT ALL CHARTS)
 * ============================================================================== */
window.initAllCharts = function(data) {
  if (!data) return;

  // Hủy các biểu đồ cũ để tránh chồng lấn hoặc rò rỉ bộ nhớ
  Object.values(chartInstances).flat().forEach(c => {
    if (c && typeof c.destroy === 'function') c.destroy();
  });
  chartInstances = {};

  if (data.monthly) chartInstances.monthly = initMonthlyTripsChart(data.monthly);
  if (data.borough) chartInstances.borough = initBoroughChart(data.borough);
  if (data.top_zones) chartInstances.topZones = initTopZonesChart(data.top_zones);
  if (data.hourly) chartInstances.hourly = initHourlyComboChart(data.hourly);
  if (data.cancelled) {
    chartInstances.cancelled = initCancelledCharts(data.cancelled, data.kpis?.total_records);
  }
  if (data.ml && data.ml.chart_data) {
    chartInstances.forecast = initForecastChart(data.ml.chart_data);
  }
};

/* ==============================================================================
 * 1. BIỂU ĐỒ SẢN LƯỢNG CHUYẾN THEO THÁNG (188 THÁNG LIÊN TỤC)
 * ============================================================================== */
function initMonthlyTripsChart(monthlyData) {
  const canvas = document.getElementById('chartMonthlyTrips');
  if (!canvas || !monthlyData) return null;

  const ctx = canvas.getContext('2d');
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, '#38bdf8');
  gradient.addColorStop(1, '#1d4ed8');

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: monthlyData.labels,
      datasets: [{
        label: 'Số chuyến taxi',
        data: monthlyData.counts,
        backgroundColor: gradient,
        hoverBackgroundColor: '#60a5fa',
        borderRadius: 3,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(10, 16, 28, 0.95)',
          titleColor: '#ffffff',
          bodyColor: '#cbd5e1',
          borderColor: 'rgba(56, 189, 248, 0.3)',
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            label: item => item.raw == null
              ? 'Không có dữ liệu'
              : ` Lưu lượng: ${Number(item.raw).toLocaleString('vi-VN')} chuyến`
          }
        }
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            color: '#94a3b8',
            maxTicksLimit: 16,
            maxRotation: 0,
            font: { size: 11 }
          }
        },
        y: {
          beginAtZero: true,
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: {
            color: '#94a3b8',
            callback: v => `${(v / 1000000).toFixed(1)} tr`
          }
        }
      }
    }
  });
}

/* ==============================================================================
 * 2. BIỂU ĐỒ DOUGHNUT THỊ PHẦN QUẬN (BOROUGH SHARE)
 * ============================================================================== */
function initBoroughChart(boroughData) {
  const canvas = document.getElementById('chartBorough');
  if (!canvas || !boroughData) return null;

  const labels = boroughData.labels.slice(0, 5);
  const counts = boroughData.counts.slice(0, 5).map(value => Number(value) || 0);
  const total = counts.reduce((sum, value) => sum + value, 0);
  const colors = ['#38bdf8', '#f59e0b', '#10b981', '#a855f7', '#64748b'];
  if (!total) {
    labels.splice(0, labels.length, 'Chưa có dữ liệu');
    counts.splice(0, counts.length, 1);
    colors.splice(1);
    colors[0] = '#334155';
  }
  const legendLabels = total
    ? labels.map((label, index) => `${label} (${formatChartPercent(counts[index] / total * 100)}%)`)
    : labels;

  return new Chart(canvas.getContext('2d'), {
    type: 'doughnut',
    plugins: [createTinySliceMarkerPlugin(counts, colors)],
    data: {
      labels: legendLabels,
      datasets: [{
        data: counts,
        backgroundColor: colors,
        borderColor: '#0e1525',
        borderWidth: 3,
        hoverOffset: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '70%',
      plugins: {
        legend: {
          position: 'right',
          labels: {
            color: '#cbd5e1',
            font: { size: 12.5 },
            boxWidth: 14,
            padding: 14
          }
        },
        tooltip: {
          backgroundColor: 'rgba(10, 16, 28, 0.95)',
          titleColor: '#ffffff',
          bodyColor: '#cbd5e1',
          borderColor: 'rgba(255, 255, 255, 0.15)',
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            label: function(item) {
              const count = total ? counts[item.dataIndex] : 0;
              return ` ${item.label}: ${Number(count).toLocaleString('vi-VN')} chuyến`;
            }
          }
        }
      }
    }
  });
}

/* ==============================================================================
 * 3. BIỂU ĐỒ BAR NGANG TOP 8 KHU VỰC ĐÓN XE ĐÔNG NHẤT
 * ============================================================================== */
function initTopZonesChart(topZonesData) {
  const canvas = document.getElementById('chartTopZones');
  if (!canvas || !topZonesData) return null;

  const ctx = canvas.getContext('2d');
  const gradient = ctx.createLinearGradient(0, 0, 400, 0);
  gradient.addColorStop(0, '#2563eb');
  gradient.addColorStop(1, '#38bdf8');

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: topZonesData.labels,
      datasets: [{
        label: 'Số lượt đón xe',
        data: topZonesData.counts,
        backgroundColor: gradient,
        hoverBackgroundColor: '#60a5fa',
        borderRadius: 5
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(10, 16, 28, 0.95)',
          titleColor: '#ffffff',
          bodyColor: '#cbd5e1',
          borderColor: 'rgba(56, 189, 248, 0.3)',
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            label: item => ` Lưu lượng: ${Number(item.raw).toLocaleString('vi-VN')} lượt đón`
          }
        }
      },
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: {
            color: '#94a3b8',
            callback: v => `${(v / 1000000).toFixed(1)} tr`
          }
        },
        y: {
          grid: { display: false },
          ticks: {
            color: '#f1f5f9',
            font: { size: 12 }
          }
        }
      }
    }
  });
}

/* ==============================================================================
 * 4. BIỂU ĐỒ ĐƯỜNG KÉP GIÁ CƯỚC & THỜI LƯỢNG 24H (HOURLY COMBO)
 * ============================================================================== */
function initHourlyComboChart(hourlyData) {
  const canvas = document.getElementById('chartHourlyCombo');
  if (!canvas || !hourlyData) return null;

  return new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels: hourlyData.hours,
      datasets: [
        {
          label: 'Giá cước bình quân (USD)',
          data: hourlyData.avg_fare,
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245, 158, 11, 0.1)',
          yAxisID: 'yFare',
          tension: 0.35,
          borderWidth: 3,
          pointRadius: 4,
          pointHoverRadius: 7,
          pointBackgroundColor: '#f59e0b'
        },
        {
          label: 'Thời gian di chuyển bình quân (Phút)',
          data: hourlyData.avg_duration,
          borderColor: '#38bdf8',
          backgroundColor: 'rgba(56, 189, 248, 0.15)',
          fill: true,
          yAxisID: 'yDuration',
          tension: 0.35,
          borderWidth: 3,
          pointRadius: 4,
          pointHoverRadius: 7,
          pointBackgroundColor: '#38bdf8'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false
      },
      plugins: {
        legend: {
          labels: {
            color: '#cbd5e1',
            font: { size: 12.5 },
            boxWidth: 14,
            padding: 16
          }
        },
        tooltip: {
          backgroundColor: 'rgba(10, 16, 28, 0.95)',
          titleColor: '#ffffff',
          bodyColor: '#cbd5e1',
          borderColor: 'rgba(255, 255, 255, 0.15)',
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            label: item => {
              const label = item.dataset.label || '';
              const val = Number(item.raw).toFixed(2);
              return item.datasetIndex === 0 ? ` ${label}: $${val}` : ` ${label}: ${val} phút`;
            }
          }
        }
      },
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: { color: '#94a3b8' }
        },
        yFare: {
          type: 'linear',
          position: 'left',
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: {
            color: '#f59e0b',
            callback: v => `$${v}`
          },
          title: {
            display: true,
            text: 'Giá cước (USD)',
            color: '#f59e0b',
            font: { weight: 700 }
          }
        },
        yDuration: {
          type: 'linear',
          position: 'right',
          grid: { display: false },
          ticks: {
            color: '#38bdf8',
            callback: v => `${v}p`
          },
          title: {
            display: true,
            text: 'Thời gian (Phút)',
            color: '#38bdf8',
            font: { weight: 700 }
          }
        }
      }
    }
  });
}

/* ==============================================================================
 * 5. CÁC BIỂU ĐỒ PHÂN TÍCH CHUYẾN BỊ HỦY / DỮ LIỆU NGOẠI LỆ
 * ============================================================================== */
function initCancelledCharts(cancelledData, cleanRecordCount) {
  const charts = [];

  // 5.1 Biểu đồ số chuyến hủy theo 24 giờ
  const canvasHour = document.getElementById('chartCancelledHours');
  if (canvasHour && cancelledData && cancelledData.hourly) {
    const hours = Array.from({ length: 24 }, (_, i) => `${i}h`);
    charts.push(new Chart(canvasHour.getContext('2d'), {
      type: 'bar',
      data: {
        labels: hours,
        datasets: [{
          label: 'Số chuyến cước âm / hủy',
          data: cancelledData.hourly,
          backgroundColor: '#f43f5e',
          hoverBackgroundColor: '#e11d48',
          borderRadius: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(10, 16, 28, 0.95)',
            titleColor: '#ffffff',
            bodyColor: '#cbd5e1',
            borderColor: 'rgba(244, 63, 94, 0.3)',
            borderWidth: 1,
            padding: 12,
            cornerRadius: 8,
            callbacks: {
              label: v => ` ${Number(v.raw).toLocaleString('vi-VN')} chuyến bị hủy / âm`
            }
          }
        },
        scales: {
          x: {
            grid: { display: false },
            ticks: { color: '#94a3b8' }
          },
          y: {
            grid: { color: 'rgba(255, 255, 255, 0.06)' },
            ticks: {
              color: '#94a3b8',
              callback: v => Number(v).toLocaleString('vi-VN')
            }
          }
        }
      }
    }));
  }

  // 5.2 Biểu đồ Doughnut tỷ lệ dữ liệu sạch vs ngoại lai
  const canvasYear = document.getElementById('chartCancelledYears');
  if (canvasYear && cancelledData) {
    const cleanTrips = Math.max(0, Number(cleanRecordCount) || 0);
    const cancelledTrips = Math.max(0, Number(cancelledData.total_cancelled) || 0);
    const rawTotal = Math.max(0, Number(cancelledData.total_raw) || 0);
    const otherExcluded = Math.max(0, rawTotal - cleanTrips - cancelledTrips);
    const categoryCounts = [cleanTrips, cancelledTrips, otherExcluded];
    const categoryNames = ['Hợp lệ & Sạch', 'Cước âm / Hủy', 'Ngoại lệ khác'];
    const categoryColors = ['#10b981', '#f43f5e', '#f59e0b'];
    const total = categoryCounts.reduce((sum, value) => sum + value, 0);
    const labels = total
      ? categoryNames.map((name, index) => (
        `${name} (${formatChartPercent(categoryCounts[index] / total * 100)}%)`
      ))
      : ['Chưa có dữ liệu'];
    const chartCounts = total ? categoryCounts : [1];
    const chartColors = total ? categoryColors : ['#334155'];
    charts.push(new Chart(canvasYear.getContext('2d'), {
      type: 'doughnut',
      plugins: [createTinySliceMarkerPlugin(categoryCounts, categoryColors)],
      data: {
        labels,
        datasets: [{
          data: chartCounts,
          backgroundColor: chartColors,
          borderColor: '#0e1525',
          borderWidth: 3,
          hoverOffset: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '68%',
        plugins: {
          legend: {
            position: 'right',
            labels: {
              color: '#cbd5e1',
              font: { size: 12.5 },
              boxWidth: 14,
              padding: 16
            }
          },
          tooltip: {
            backgroundColor: 'rgba(10, 16, 28, 0.95)',
            titleColor: '#ffffff',
            bodyColor: '#cbd5e1',
            borderColor: 'rgba(255, 255, 255, 0.15)',
            borderWidth: 1,
            padding: 12,
            cornerRadius: 8,
            callbacks: {
              label: item => {
                const count = total ? categoryCounts[item.dataIndex] : 0;
                return ` ${item.label}: ${Number(count).toLocaleString('vi-VN')} bản ghi`;
              }
            }
          }
        }
      },
    }));
  }

  return charts;
}

/* ==============================================================================
 * 6. BIỂU ĐỒ CHUỖI THỜI GIAN SO SÁNH ACTUAL VS FORECAST (MACHINE LEARNING)
 * ============================================================================== */
function initForecastChart(mlChartData) {
  const canvas = document.getElementById('chartForecast');
  if (!canvas || !mlChartData) return null;

  return new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels: mlChartData.time_labels,
      datasets: [
        {
          label: 'Nhu cầu thực tế (Actual Label)',
          data: mlChartData.actual,
          borderColor: '#ffffff',
          backgroundColor: 'transparent',
          borderWidth: 2.5,
          borderDash: [5, 4],
          pointRadius: 4,
          pointBackgroundColor: '#ffffff'
        },
        {
          label: 'Dự báo Baseline (Trung bình)',
          data: mlChartData.baseline,
          borderColor: '#64748b',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 3,
          pointBackgroundColor: '#64748b',
          tension: 0.15
        },
        {
          label: 'Dự báo GBTRegressor (Spark MLlib)',
          data: mlChartData.gbt,
          borderColor: '#10b981',
          backgroundColor: 'rgba(16, 185, 129, 0.1)',
          fill: true,
          borderWidth: 3,
          pointRadius: 4,
          pointBackgroundColor: '#10b981',
          tension: 0.3
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false
      },
      plugins: {
        legend: {
          position: 'top',
          labels: {
            color: '#cbd5e1',
            font: { size: 12.5 },
            boxWidth: 14,
            padding: 16
          }
        },
        tooltip: {
          backgroundColor: 'rgba(10, 16, 28, 0.95)',
          titleColor: '#ffffff',
          bodyColor: '#cbd5e1',
          borderColor: 'rgba(16, 185, 129, 0.4)',
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8,
          callbacks: {
            label: v => ` ${v.dataset.label}: ${Number(v.raw).toFixed(1)} chuyến/giờ`
          }
        }
      },
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: {
            color: '#94a3b8',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 12
          }
        },
        y: {
          grid: { color: 'rgba(255, 255, 255, 0.06)' },
          ticks: { color: '#94a3b8' },
          title: {
            display: true,
            text: 'Số chuyến / giờ',
            color: '#cbd5e1',
            font: { weight: 700 }
          }
        }
      }
    }
  });
}
